package batch

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"

	"github.com/koriebruh/Jengine/internal/domain"
	"github.com/koriebruh/Jengine/internal/matching/core"
)

// WriteResults persists outcome: batch-inserts MatchResult+Lines for
// auto-matched/suggested candidates (via domain.MatchResultRepository.Create,
// which itself uses a chunked multi-row INSERT for the lines - never
// row-by-row, plans/task/core/05/12), batch-updates Transaction.status
// per outcome type in a single UPDATE...WHERE id = ANY($1), and opens one
// Break per unmatched transaction (MVP default - grouped/bulk break
// creation is a documented possible refinement, not required for MVP).
//
// Consistency boundary (documented here since it's a common source of
// subtle bugs, per plans/task/core/12 Implementation Notes): the
// MatchResult/Transaction.status writes below happen inside ONE
// transaction (via deps.TxRunner), so a partial-partition failure can't
// leave transactions half-updated. BreakSink.OpenBreak calls happen
// OUTSIDE that transaction, deliberately - case creation
// (plans/task/core/13) is its own bounded operation with its own
// consistency needs, not something this worker's transaction should
// hold open for.
//
// txByID must contain every transaction referenced by outcome (both
// sides of every candidate, plus every unmatched ID) - the caller
// (PartitionWorker.Work) already has this from its own load step.
func WriteResults(ctx context.Context, deps WorkerDeps, tenantID uuid.UUID, outcome core.MatchOutcome, txByID map[uuid.UUID]domain.Transaction) error {
	err := deps.TxRunner(ctx, tenantID, func(ctx context.Context) error {
		var matchedIDs []uuid.UUID

		for _, cand := range outcome.AutoMatched {
			if err := writeMatchResult(ctx, deps, tenantID, cand, domain.MatchResultStatusAutoMatched, txByID); err != nil {
				return err
			}
			matchedIDs = append(matchedIDs, cand.SourceIDs...)
			matchedIDs = append(matchedIDs, cand.TargetIDs...)
		}

		// Suggested candidates are NOT yet matched - the constituent
		// transactions stay UNMATCHED (visible for analyst review) until
		// an analyst confirms; only the MatchResult row itself carries
		// SUGGESTED status. Treating a suggestion's transactions as
		// matched (or, conversely, as Break-worthy residue) is exactly
		// the mistake plans/task/core/12 Common Pitfalls warns against.
		for _, cand := range outcome.Suggested {
			if err := writeMatchResult(ctx, deps, tenantID, cand, domain.MatchResultStatusSuggested, txByID); err != nil {
				return err
			}
		}

		if len(matchedIDs) > 0 {
			if err := deps.Transactions.BulkUpdateStatus(ctx, tenantID, matchedIDs, domain.TransactionStatusMatched); err != nil {
				return fmt.Errorf("bulk update auto-matched status: %w", err)
			}
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("batch: write results: %w", err)
	}

	if deps.BreakSink == nil {
		return nil
	}
	for _, id := range outcome.Unmatched {
		tx, ok := txByID[id]
		if !ok {
			continue // defensive - shouldn't happen if the caller built txByID correctly
		}
		if err := deps.BreakSink.OpenBreak(ctx, core.OpenBreakParams{
			TenantID:       tenantID,
			AccountID:      tx.AccountID,
			TransactionIDs: []uuid.UUID{id},
			BreakType:      "UNMATCHED",
			AmountAtRisk:   tx.BaseAmount,
			Currency:       tx.Currency,
		}); err != nil {
			return fmt.Errorf("batch: open break for transaction %s: %w", id, err)
		}
	}

	// Fired after every write above (MatchResults, Transaction.status,
	// Breaks) completes - plans/task/core/19's reconciliation hook, see
	// WorkerDeps.PostWrite's own doc comment. Optional; most callers
	// (including every test predating task 19) leave it nil.
	if deps.PostWrite != nil {
		if err := deps.PostWrite(ctx, tenantID, outcome, txByID); err != nil {
			return fmt.Errorf("batch: post-write reconciliation hook: %w", err)
		}
	}
	return nil
}

func writeMatchResult(ctx context.Context, deps WorkerDeps, tenantID uuid.UUID, cand core.ScoredCandidate, status domain.MatchResultStatus, txByID map[uuid.UUID]domain.Transaction) error {
	cardinality := domain.MatchCardinalityOneToOne
	switch {
	case len(cand.SourceIDs) > 1:
		cardinality = domain.MatchCardinalityManyToOne
	case len(cand.TargetIDs) > 1:
		cardinality = domain.MatchCardinalityOneToMany
	}

	ruleID := cand.RuleID
	result := domain.MatchResult{
		RuleID:          &ruleID,
		MatchType:       cardinality,
		ConfidenceScore: decimal.NewFromFloat(cand.Score),
		Status:          status,
		MatchedAt:       time.Now(),
	}

	lines := make([]domain.MatchResultLine, 0, len(cand.SourceIDs)+len(cand.TargetIDs))
	for _, id := range cand.SourceIDs {
		lines = append(lines, domain.MatchResultLine{
			TransactionID: id, TenantID: tenantID,
			Side: domain.MatchResultLineSideSource, AllocatedAmount: allocatedAmount(txByID, id),
		})
	}
	for _, id := range cand.TargetIDs {
		lines = append(lines, domain.MatchResultLine{
			TransactionID: id, TenantID: tenantID,
			Side: domain.MatchResultLineSideTarget, AllocatedAmount: allocatedAmount(txByID, id),
		})
	}

	_, err := deps.MatchResults.Create(ctx, tenantID, result, lines)
	if err != nil {
		return fmt.Errorf("write match result for rule %s: %w", cand.RuleID, err)
	}
	return nil
}

func allocatedAmount(txByID map[uuid.UUID]domain.Transaction, id uuid.UUID) decimal.Decimal {
	if tx, ok := txByID[id]; ok {
		return tx.BaseAmount
	}
	return decimal.Zero
}
