// Package reconcile implements the batch/streaming hybrid reconciliation
// job (plans/task/core/19, plans/docs/06-streaming-architecture.md §7.5):
// after a batch pass completes, this is what promotes a provisional
// AUTO_MATCHED_STREAMING match to the final AUTO_MATCHED_CONFIRMED
// status when the batch pass agrees (concordant), or opens a lightweight
// RECONCILIATION_VARIANCE Break when it doesn't (discordant). This is
// deliberately the harder, more important half of task 19 - get it
// wrong and either the "continuous reconciliation" story breaks, or the
// platform silently produces false-confidence matches.
package reconcile

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"

	"github.com/koriebruh/Jengine/internal/cases"
	"github.com/koriebruh/Jengine/internal/domain"
	"github.com/koriebruh/Jengine/internal/matching/core"
	"github.com/koriebruh/Jengine/internal/platform/outbox"
	"github.com/koriebruh/Jengine/internal/storage/postgres"
)

// TxRunner wraps fn in a transaction scoped to tenantID - same shape as
// every other package's own local copy in this codebase. Must establish
// the SAME ambient-tx-in-context convention postgres.TxFromContext reads
// (i.e. be backed by postgres.WithTx) since confirmConcordant needs the
// raw pgx.Tx to write an outbox_event row atomically with the status
// update.
type TxRunner func(ctx context.Context, tenantID uuid.UUID, fn func(ctx context.Context) error) error

// Deps are the Reconciler's dependencies.
type Deps struct {
	TxRunner     TxRunner
	MatchResults domain.MatchResultRepository
	Cases        domain.CaseRepository
	Lifecycle    cases.LifecycleService
}

// Reconciler runs ReconcileBatchAgainstStream.
type Reconciler struct {
	Deps Deps
}

// batchCandidate is the minimal shape Reconciler needs from a batch
// pass's own auto-matched output - a subset of core.ScoredCandidate,
// named separately so this package's public API doesn't force callers
// to depend on core.ScoredCandidate's exact shape if it ever changes for
// batch-only reasons.
type batchCandidate = core.ScoredCandidate

// ReconcileBatchAgainstStream is triggered right after a batch pass
// completes for one partition (tenant, account pair, value-date bucket) -
// wired as a hook the batch worker fires on partition completion
// (batch.WorkerDeps.PostWrite), not a rewrite of the batch worker
// itself. batchOutcome and txByID are the SAME values the batch worker
// just computed/wrote via WriteResults - passing them directly avoids
// needing to re-query "which MatchResults belong to this partition"
// from the DB, a query this schema has no direct support for.
func (r *Reconciler) ReconcileBatchAgainstStream(ctx context.Context, tenantID uuid.UUID, batchOutcome core.MatchOutcome, txByID map[uuid.UUID]domain.Transaction) error {
	partitionTxIDs := make(map[uuid.UUID]bool, len(txByID))
	for id := range txByID {
		partitionTxIDs[id] = true
	}

	streamingResults, err := r.loadOverlappingStreamingResults(ctx, tenantID, partitionTxIDs)
	if err != nil {
		return fmt.Errorf("reconcile: load streaming results: %w", err)
	}

	consumed := make(map[uuid.UUID]bool, len(streamingResults))

	for _, batchCand := range batchOutcome.AutoMatched {
		batchTxIDs := candidateTxIDSet(batchCand)

		var overlapping *streamingResultWithLines
		for i := range streamingResults {
			sr := &streamingResults[i]
			if consumed[sr.result.ID] {
				continue
			}
			if setsOverlap(sr.txIDs, batchTxIDs) {
				overlapping = sr
				break
			}
		}
		if overlapping == nil {
			// Batch found this, streaming never touched it - the
			// expected common case (plans/task/core/19 Implementation
			// Notes point 4), not a discordance.
			continue
		}
		consumed[overlapping.result.ID] = true

		if setsEqual(overlapping.txIDs, batchTxIDs) {
			if err := r.confirmConcordant(ctx, tenantID, overlapping.result, batchCand); err != nil {
				return err
			}
		} else {
			if err := r.openVarianceBreak(ctx, tenantID, txByID, overlapping.result, overlapping.txIDs, &batchCand); err != nil {
				return err
			}
		}
	}

	// Any streaming result touching this partition that no batch
	// candidate consumed: streaming matched something the batch pass
	// didn't confirm (batch either left those transactions Unmatched,
	// or grouped them into a DIFFERENT candidate already consumed
	// above by a different streaming result) - both are discordant per
	// plans/task/core/19 Implementation Notes point 3.
	for i := range streamingResults {
		sr := &streamingResults[i]
		if consumed[sr.result.ID] {
			continue
		}
		if err := r.openVarianceBreak(ctx, tenantID, txByID, sr.result, sr.txIDs, nil); err != nil {
			return err
		}
	}

	return nil
}

func (r *Reconciler) confirmConcordant(ctx context.Context, tenantID uuid.UUID, streamingResult domain.MatchResult, batchCand batchCandidate) error {
	return r.Deps.TxRunner(ctx, tenantID, func(ctx context.Context) error {
		if err := r.Deps.MatchResults.UpdateStatus(ctx, tenantID, streamingResult.ID, domain.MatchResultStatusAutoMatchedConfirmed, nil); err != nil {
			return fmt.Errorf("confirm streaming match result %s: %w", streamingResult.ID, err)
		}

		payload, err := json.Marshal(map[string]any{
			"match_result_id": streamingResult.ID,
			"tenant_id":       tenantID,
			"confirmed_at":    time.Now().UTC(),
		})
		if err != nil {
			return fmt.Errorf("marshal match.auto_confirmed payload: %w", err)
		}
		// TxRunner (backed by postgres.WithTx, per this package's own
		// TxRunner doc comment) puts the tx in context via
		// postgres.ContextWithTx - retrieve it here so outbox.Insert
		// writes the event in the SAME transaction as the status
		// update above (task 18's outbox pattern: state change + event
		// emission must be atomic).
		tx, ok := postgres.TxFromContext(ctx)
		if !ok {
			return fmt.Errorf("reconcile: no pgx.Tx in context to write outbox event")
		}
		return outbox.Insert(ctx, tx, tenantID, outbox.Event{
			AggregateType: "match_result", AggregateID: streamingResult.ID,
			EventType: "match.auto_confirmed", Topic: "case.events.default", Payload: payload,
		})
	})
}

// openVarianceBreak opens a RECONCILIATION_VARIANCE Break and attaches a
// CaseAuditEvent snapshotting both the streaming result and the batch
// candidate (if any) - "the system must show exactly what changed"
// (plans/task/core/19 Implementation Notes) via this snapshot, not a
// re-investigation. batchCand is nil when streaming found a match batch
// never confirmed at all (no batch candidate to snapshot).
func (r *Reconciler) openVarianceBreak(ctx context.Context, tenantID uuid.UUID, txByID map[uuid.UUID]domain.Transaction, streamingResult domain.MatchResult, streamingTxIDs map[uuid.UUID]bool, batchCand *batchCandidate) error {
	var accountID uuid.UUID
	var amountAtRisk decimal.Decimal
	var currency string
	var allTxIDs []uuid.UUID
	for id := range streamingTxIDs {
		allTxIDs = append(allTxIDs, id)
		if tx, ok := txByID[id]; ok {
			accountID = tx.AccountID
			amountAtRisk = tx.BaseAmount
			currency = tx.Currency
		}
	}

	diff := map[string]any{
		"streaming_result_id":       streamingResult.ID,
		"streaming_confidence":      streamingResult.ConfidenceScore,
		"streaming_transaction_ids": allTxIDs,
	}
	if batchCand != nil {
		diff["batch_rule_id"] = batchCand.RuleID
		diff["batch_confidence"] = batchCand.Score
		diff["batch_source_transaction_ids"] = batchCand.SourceIDs
		diff["batch_target_transaction_ids"] = batchCand.TargetIDs
	} else {
		diff["batch_outcome"] = "no_confirming_match_found"
	}
	payload, err := json.Marshal(diff)
	if err != nil {
		return fmt.Errorf("marshal reconciliation variance diff: %w", err)
	}

	openedCase, err := r.Deps.Lifecycle.OpenBreak(ctx, cases.OpenBreakParams{
		TenantID: tenantID, AccountID: accountID, TransactionIDs: allTxIDs,
		BreakType:    string(domain.BreakTypeReconciliationVariance),
		AmountAtRisk: amountAtRisk, Currency: currency,
	})
	if err != nil {
		return fmt.Errorf("open reconciliation_variance break: %w", err)
	}

	// AddComment/AddAuditEvent go through CaseRepository directly - this
	// package depends on cases.LifecycleService for OpenBreak (state-
	// machine semantics) but the audit-event diff attachment is plain
	// CRUD, not a lifecycle transition, so it's fine to add via the
	// repository the lifecycle service itself is presumably backed by.
	// (Deps carries Lifecycle, not a separate CaseRepository, to avoid
	// two ways to open/mutate a case - see AddDiffAuditEvent below for
	// where this actually gets called from.)
	return r.addDiffAuditEvent(ctx, tenantID, openedCase.ID, payload)
}

// addDiffAuditEvent is a separate method (not inlined into
// openVarianceBreak) since it needs domain.CaseRepository specifically,
// not cases.LifecycleService - see AttachAuditEvents field.
func (r *Reconciler) addDiffAuditEvent(ctx context.Context, tenantID, caseID uuid.UUID, payload json.RawMessage) error {
	if r.Deps.Cases == nil {
		return nil
	}
	err := r.Deps.TxRunner(ctx, tenantID, func(ctx context.Context) error {
		_, err := r.Deps.Cases.AddAuditEvent(ctx, tenantID, domain.CaseAuditEvent{
			CaseID: caseID, Actor: "system:reconciler", EventType: "reconciliation_variance_detected", Payload: payload,
		})
		return err
	})
	if err != nil {
		return fmt.Errorf("attach reconciliation variance diff audit event: %w", err)
	}
	return nil
}

type streamingResultWithLines struct {
	result domain.MatchResult
	txIDs  map[uuid.UUID]bool
}

func (r *Reconciler) loadOverlappingStreamingResults(ctx context.Context, tenantID uuid.UUID, partitionTxIDs map[uuid.UUID]bool) ([]streamingResultWithLines, error) {
	var all []domain.MatchResult
	err := r.Deps.TxRunner(ctx, tenantID, func(ctx context.Context) error {
		var err error
		all, err = r.Deps.MatchResults.ListByStatus(ctx, tenantID, domain.MatchResultStatusAutoMatchedStreaming)
		return err
	})
	if err != nil {
		return nil, err
	}

	var overlapping []streamingResultWithLines
	for _, mr := range all {
		var lines []domain.MatchResultLine
		err := r.Deps.TxRunner(ctx, tenantID, func(ctx context.Context) error {
			_, l, err := r.Deps.MatchResults.GetByID(ctx, tenantID, mr.ID)
			lines = l
			return err
		})
		if err != nil {
			return nil, fmt.Errorf("load lines for streaming result %s: %w", mr.ID, err)
		}
		txIDs := make(map[uuid.UUID]bool, len(lines))
		touchesPartition := false
		for _, line := range lines {
			txIDs[line.TransactionID] = true
			if partitionTxIDs[line.TransactionID] {
				touchesPartition = true
			}
		}
		if touchesPartition {
			overlapping = append(overlapping, streamingResultWithLines{result: mr, txIDs: txIDs})
		}
	}
	return overlapping, nil
}

func candidateTxIDSet(cand batchCandidate) map[uuid.UUID]bool {
	set := make(map[uuid.UUID]bool, len(cand.SourceIDs)+len(cand.TargetIDs))
	for _, id := range cand.SourceIDs {
		set[id] = true
	}
	for _, id := range cand.TargetIDs {
		set[id] = true
	}
	return set
}

func setsEqual(a, b map[uuid.UUID]bool) bool {
	if len(a) != len(b) {
		return false
	}
	for id := range a {
		if !b[id] {
			return false
		}
	}
	return true
}

func setsOverlap(a, b map[uuid.UUID]bool) bool {
	for id := range a {
		if b[id] {
			return true
		}
	}
	return false
}
