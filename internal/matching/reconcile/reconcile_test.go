package reconcile_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"

	"github.com/koriebruh/Jengine/internal/audit"
	"github.com/koriebruh/Jengine/internal/cases"
	"github.com/koriebruh/Jengine/internal/domain"
	"github.com/koriebruh/Jengine/internal/matching/core"
	"github.com/koriebruh/Jengine/internal/matching/reconcile"
	"github.com/koriebruh/Jengine/internal/storage/postgres"
	"github.com/koriebruh/Jengine/internal/tenancy"
	"github.com/koriebruh/Jengine/internal/testutil"
)

func TestReconciler_ReconcileBatchAgainstStream(t *testing.T) {
	if testing.Short() {
		t.Skip("requires Docker - skipped under -short; run make test-integration")
	}

	db := testutil.StartPostgres(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	tenantID := uuid.New()
	if _, err := db.Pool.Exec(ctx,
		`INSERT INTO tenants (id, name, isolation_tier, region, status) VALUES ($1, 'Acme', 'STANDARD', 'us-east', 'ACTIVE')`,
		tenantID,
	); err != nil {
		t.Fatalf("seed tenant failed: %v", err)
	}

	accountID := uuid.New()
	if _, err := db.Pool.Exec(ctx,
		`INSERT INTO accounts (id, tenant_id, external_account_ref, account_type, currency, name) VALUES ($1, $2, $3, 'BANK', 'USD', 'Account')`,
		accountID, tenantID, accountID.String(),
	); err != nil {
		t.Fatalf("seed account failed: %v", err)
	}

	appPool := testutil.AppRolePool(t, ctx, db.DSN)
	defer appPool.Close()

	txRunner := func(ctx context.Context, tid uuid.UUID, fn func(ctx context.Context) error) error {
		return postgres.WithTx(tenancy.WithTenant(ctx, tenancy.TenantContext{TenantID: tid}), appPool, tid, fn)
	}
	casesTxRunner := func(ctx context.Context, tid uuid.UUID, fn func(ctx context.Context) error) error {
		return postgres.WithTx(tenancy.WithTenant(ctx, tenancy.TenantContext{TenantID: tid}), appPool, tid, fn)
	}

	day := time.Now().Truncate(24 * time.Hour)
	insertTx := func(ref string, amount string) uuid.UUID {
		t.Helper()
		id := uuid.New()
		if _, err := db.Pool.Exec(ctx,
			`INSERT INTO transactions (id, tenant_id, account_id, external_ref, amount, currency, base_amount, value_date, booking_date, side, source_mode, ingestion_idempotency_key, status)
			 VALUES ($1, $2, $3, $4, $5, 'USD', $5, $6, $6, 'DEBIT', 'STREAM', $7, 'UNMATCHED')`,
			id, tenantID, accountID, ref, amount, day, id.String(),
		); err != nil {
			t.Fatalf("seed transaction failed: %v", err)
		}
		return id
	}

	matchResults := postgres.NewMatchResultRepo()
	caseRepo := postgres.NewCaseRepo()
	lifecycle := cases.NewPostgresLifecycleService(casesTxRunner, caseRepo, audit.NewPostgresWriter())
	reconciler := &reconcile.Reconciler{
		Deps: reconcile.Deps{
			TxRunner: txRunner, MatchResults: matchResults, Cases: caseRepo, Lifecycle: lifecycle,
		},
	}

	insertStreamingResult := func(txIDs ...uuid.UUID) uuid.UUID {
		t.Helper()
		var lines []domain.MatchResultLine
		for i, id := range txIDs {
			side := domain.MatchResultLineSideSource
			if i > 0 {
				side = domain.MatchResultLineSideTarget
			}
			lines = append(lines, domain.MatchResultLine{TransactionID: id, TenantID: tenantID, Side: side})
		}
		var created domain.MatchResult
		err := txRunner(ctx, tenantID, func(ctx context.Context) error {
			var err error
			created, err = matchResults.Create(ctx, tenantID, domain.MatchResult{
				MatchType: domain.MatchCardinalityOneToOne, ConfidenceScore: decimal.NewFromFloat(0.95),
				Status: domain.MatchResultStatusAutoMatchedStreaming, MatchedAt: time.Now(),
			}, lines)
			return err
		})
		if err != nil {
			t.Fatalf("seed streaming match result failed: %v", err)
		}
		return created.ID
	}

	t.Run("concordant: batch confirms streaming match", func(t *testing.T) {
		src, tgt := insertTx("REF-CONCORDANT", "100.00"), insertTx("REF-CONCORDANT", "100.00")
		streamingID := insertStreamingResult(src, tgt)

		txByID := map[uuid.UUID]domain.Transaction{
			src: {ID: src, AccountID: accountID, BaseAmount: decimal.NewFromInt(100), Currency: "USD"},
			tgt: {ID: tgt, AccountID: accountID, BaseAmount: decimal.NewFromInt(100), Currency: "USD"},
		}
		batchOutcome := core.MatchOutcome{
			AutoMatched: []core.ScoredCandidate{{RuleID: uuid.New(), SourceIDs: []uuid.UUID{src}, TargetIDs: []uuid.UUID{tgt}, Score: 0.98}},
		}

		if err := reconciler.ReconcileBatchAgainstStream(ctx, tenantID, batchOutcome, txByID); err != nil {
			t.Fatalf("ReconcileBatchAgainstStream failed: %v", err)
		}

		var status string
		if err := db.Pool.QueryRow(ctx, `SELECT status FROM match_results WHERE id = $1`, streamingID).Scan(&status); err != nil {
			t.Fatalf("query status failed: %v", err)
		}
		if status != string(domain.MatchResultStatusAutoMatchedConfirmed) {
			t.Errorf("expected AUTO_MATCHED_CONFIRMED, got %s", status)
		}

		var outboxCount int
		if err := db.Pool.QueryRow(ctx, `SELECT count(*) FROM outbox_event WHERE aggregate_id = $1 AND event_type = 'match.auto_confirmed'`, streamingID).Scan(&outboxCount); err != nil {
			t.Fatalf("query outbox_event failed: %v", err)
		}
		if outboxCount != 1 {
			t.Errorf("expected 1 match.auto_confirmed outbox event, got %d", outboxCount)
		}
	})

	t.Run("discordant: streaming found no counterpart batch confirmed", func(t *testing.T) {
		orphan := insertTx("REF-ORPHAN", "50.00")
		streamingID := insertStreamingResult(orphan)

		txByID := map[uuid.UUID]domain.Transaction{
			orphan: {ID: orphan, AccountID: accountID, BaseAmount: decimal.NewFromInt(50), Currency: "USD"},
		}
		// Batch found nothing at all for this transaction (Unmatched) -
		// no AutoMatched candidate references it.
		batchOutcome := core.MatchOutcome{Unmatched: []uuid.UUID{orphan}}

		if err := reconciler.ReconcileBatchAgainstStream(ctx, tenantID, batchOutcome, txByID); err != nil {
			t.Fatalf("ReconcileBatchAgainstStream failed: %v", err)
		}

		var status string
		if err := db.Pool.QueryRow(ctx, `SELECT status FROM match_results WHERE id = $1`, streamingID).Scan(&status); err != nil {
			t.Fatalf("query status failed: %v", err)
		}
		if status != string(domain.MatchResultStatusAutoMatchedStreaming) {
			t.Errorf("expected the streaming result to stay PROVISIONAL (not silently confirmed), got %s", status)
		}

		var caseCount int
		if err := db.Pool.QueryRow(ctx, `SELECT count(*) FROM cases WHERE tenant_id = $1 AND break_type = 'RECONCILIATION_VARIANCE'`, tenantID).Scan(&caseCount); err != nil {
			t.Fatalf("query cases failed: %v", err)
		}
		if caseCount != 1 {
			t.Fatalf("expected 1 RECONCILIATION_VARIANCE case, got %d", caseCount)
		}

		var auditCount int
		if err := db.Pool.QueryRow(ctx,
			`SELECT count(*) FROM case_audit_events cae JOIN cases c ON cae.case_id = c.id
			 WHERE c.tenant_id = $1 AND c.break_type = 'RECONCILIATION_VARIANCE' AND cae.event_type = 'reconciliation_variance_detected'`,
			tenantID,
		).Scan(&auditCount); err != nil {
			t.Fatalf("query case_audit_events failed: %v", err)
		}
		if auditCount != 1 {
			t.Errorf("expected 1 reconciliation_variance_detected audit event (the diff snapshot), got %d", auditCount)
		}
	})

	t.Run("discordant: conflicting grouping between streaming and batch", func(t *testing.T) {
		a, b, c := insertTx("REF-CONFLICT", "75.00"), insertTx("REF-CONFLICT", "75.00"), insertTx("REF-CONFLICT-ALT", "75.00")
		// Streaming matched a+b.
		streamingID := insertStreamingResult(a, b)

		txByID := map[uuid.UUID]domain.Transaction{
			a: {ID: a, AccountID: accountID, BaseAmount: decimal.NewFromFloat(75), Currency: "USD"},
			b: {ID: b, AccountID: accountID, BaseAmount: decimal.NewFromFloat(75), Currency: "USD"},
			c: {ID: c, AccountID: accountID, BaseAmount: decimal.NewFromFloat(75), Currency: "USD"},
		}
		// But batch (with the full end-of-day picture) actually
		// grouped a+c instead - a genuine conflicting-grouping
		// discordance (e.g. a late-arriving counterpart c that
		// streaming's narrower window never saw).
		batchOutcome := core.MatchOutcome{
			AutoMatched: []core.ScoredCandidate{{RuleID: uuid.New(), SourceIDs: []uuid.UUID{a}, TargetIDs: []uuid.UUID{c}, Score: 0.97}},
		}

		if err := reconciler.ReconcileBatchAgainstStream(ctx, tenantID, batchOutcome, txByID); err != nil {
			t.Fatalf("ReconcileBatchAgainstStream failed: %v", err)
		}

		var status string
		if err := db.Pool.QueryRow(ctx, `SELECT status FROM match_results WHERE id = $1`, streamingID).Scan(&status); err != nil {
			t.Fatalf("query status failed: %v", err)
		}
		if status != string(domain.MatchResultStatusAutoMatchedStreaming) {
			t.Errorf("expected the streaming result to stay PROVISIONAL on conflicting grouping, got %s", status)
		}

		var caseCount int
		if err := db.Pool.QueryRow(ctx,
			`SELECT count(*) FROM cases WHERE tenant_id = $1 AND break_type = 'RECONCILIATION_VARIANCE' AND $2 = ANY(related_transaction_ids)`,
			tenantID, a,
		).Scan(&caseCount); err != nil {
			t.Fatalf("query cases failed: %v", err)
		}
		if caseCount != 1 {
			t.Fatalf("expected 1 RECONCILIATION_VARIANCE case referencing transaction %s, got %d", a, caseCount)
		}
	})
}
