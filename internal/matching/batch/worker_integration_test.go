package batch_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/shopspring/decimal"

	"github.com/koriebruh/Jengine/internal/matching/batch"
	"github.com/koriebruh/Jengine/internal/matching/core"
	"github.com/koriebruh/Jengine/internal/matching/rules"
	"github.com/koriebruh/Jengine/internal/storage/postgres"
	"github.com/koriebruh/Jengine/internal/tenancy"
	"github.com/koriebruh/Jengine/internal/testutil"
)

// fakeBreakSink is a test double for core.BreakSink - stands in for
// plans/task/core/13's real implementation, which doesn't exist yet
// (plans/task/core/12's own Prerequisites explicitly allow this
// sequencing: "task 12 and task 13 can be built in either order... as
// long as the BreakSink interface is respected").
type fakeBreakSink struct {
	opened []core.OpenBreakParams
}

func (f *fakeBreakSink) OpenBreak(ctx context.Context, params core.OpenBreakParams) error {
	f.opened = append(f.opened, params)
	return nil
}

// TestPartitionWorker_EndToEnd seeds a tenant, two accounts, a handful of
// transactions, and one active rule; runs the worker via a real River
// client (insert + start + wait); and asserts the expected MatchResult
// rows, Transaction.status updates, and Break (via fakeBreakSink) calls -
// plans/task/core/12's Definition of Done integration test.
func TestPartitionWorker_EndToEnd(t *testing.T) {
	if testing.Short() {
		t.Skip("requires Docker - skipped under -short; run make test-integration")
	}

	db := testutil.StartPostgres(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	if err := batch.EnsureRiverSchema(ctx, db.Pool); err != nil {
		t.Fatalf("EnsureRiverSchema failed: %v", err)
	}

	tenantID := uuid.New()
	if _, err := db.Pool.Exec(ctx,
		`INSERT INTO tenants (id, name, isolation_tier, region, status) VALUES ($1, 'Acme', 'STANDARD', 'us-east', 'ACTIVE')`,
		tenantID,
	); err != nil {
		t.Fatalf("seed tenant failed: %v", err)
	}

	accountA, accountB := uuid.New(), uuid.New()
	for _, id := range []uuid.UUID{accountA, accountB} {
		if _, err := db.Pool.Exec(ctx,
			`INSERT INTO accounts (id, tenant_id, external_account_ref, account_type, currency, name) VALUES ($1, $2, $3, 'BANK', 'USD', 'Account')`,
			id, tenantID, id.String(),
		); err != nil {
			t.Fatalf("seed account failed: %v", err)
		}
	}

	day := time.Date(2024, 7, 1, 0, 0, 0, 0, time.UTC)
	insertTx := func(accountID uuid.UUID, ref string, amount string) uuid.UUID {
		t.Helper()
		id := uuid.New()
		if _, err := db.Pool.Exec(ctx,
			`INSERT INTO transactions (id, tenant_id, account_id, external_ref, amount, currency, base_amount, value_date, booking_date, side, source_mode, ingestion_idempotency_key, status)
			 VALUES ($1, $2, $3, $4, $5, 'USD', $5, $6, $6, 'DEBIT', 'BATCH', $7, 'UNMATCHED')`,
			id, tenantID, accountID, ref, amount, day, id.String(),
		); err != nil {
			t.Fatalf("seed transaction failed: %v", err)
		}
		return id
	}

	// Matching pair: same reference, same amount -> should auto-match.
	matchSrc := insertTx(accountA, "REF-MATCH-001", "500.00")
	matchTgt := insertTx(accountB, "REF-MATCH-001", "500.00")
	// Unmatched leftover on accountA: no counterpart on accountB.
	unmatchedTx := insertTx(accountA, "REF-NO-MATCH-999", "12.34")

	// Seed one active rule (matching MatchRule schema).
	ruleSpec := rules.RuleSpec{}
	ruleSpec.Rule.Name = "Test rule"
	ruleSpec.Rule.Version = 1
	ruleSpec.Rule.MatchCardinality = "ONE_TO_ONE"
	ruleSpec.Rule.Keys = []rules.KeySpec{
		{Field: "currency", Tolerance: rules.ToleranceYAML{Type: "exact"}},
	}
	ruleSpec.Rule.Scoring = []rules.ScoringSpec{
		{Field: "reference", Method: "exact", Weight: 1.0},
	}
	ruleSpec.Rule.Thresholds = rules.ThresholdSpec{AutoMatch: 0.9, Suggest: 0.5}
	ruleSpec.Rule.Execution = rules.ExecutionSpec{Priority: 1}
	ruleSpecJSON, err := json.Marshal(ruleSpec)
	if err != nil {
		t.Fatalf("marshal rule spec: %v", err)
	}

	ruleID := uuid.New()
	if _, err := db.Pool.Exec(ctx,
		`INSERT INTO match_rules (id, tenant_id, name, version, status, rule_spec, match_type, source_account_id, target_account_id, priority, auto_match_threshold, created_by)
		 VALUES ($1, $2, 'Test rule', 1, 'ACTIVE', $3, 'COMPOSITE', $4, $5, 1, 0.9, 'test')`,
		ruleID, tenantID, ruleSpecJSON, accountA, accountB,
	); err != nil {
		t.Fatalf("seed match rule failed: %v", err)
	}

	appPool := testutil.AppRolePool(t, ctx, db.DSN)
	defer appPool.Close()

	txRunner := func(ctx context.Context, tid uuid.UUID, fn func(ctx context.Context) error) error {
		return postgres.WithTx(tenancy.WithTenant(ctx, tenancy.TenantContext{TenantID: tid}), appPool, tid, fn)
	}

	breakSink := &fakeBreakSink{}
	deps := batch.WorkerDeps{
		TxRunner:     txRunner,
		Transactions: postgres.NewTransactionRepo(),
		MatchResults: postgres.NewMatchResultRepo(),
		MatchRules:   postgres.NewMatchRuleRepo(),
		Registry:     rules.DefaultRegistry(),
		BreakSink:    breakSink,
	}
	worker := &batch.PartitionWorker{Deps: deps}

	riverClient, err := batch.NewRiverClient(db.Pool, worker, 2)
	if err != nil {
		t.Fatalf("NewRiverClient failed: %v", err)
	}
	if err := riverClient.Start(ctx); err != nil {
		t.Fatalf("river client Start failed: %v", err)
	}
	defer func() { _ = riverClient.Stop(ctx) }()

	partitions, err := batch.EnumeratePartitions(ctx, db.Pool, time.Time{})
	if err != nil {
		t.Fatalf("EnumeratePartitions failed: %v", err)
	}
	if len(partitions) != 1 {
		t.Fatalf("expected exactly 1 partition, got %d: %+v", len(partitions), partitions)
	}

	if err := batch.EnqueuePartitions(ctx, riverClient, partitions); err != nil {
		t.Fatalf("EnqueuePartitions failed: %v", err)
	}

	waitForJobCompletion(t, ctx, db.Pool, 15*time.Second)

	// Assert: matchSrc/matchTgt -> MATCHED, unmatchedTx -> still
	// UNMATCHED (only a Break is opened for it, its status doesn't
	// change - opening a Break doesn't itself alter Transaction.status).
	assertTransactionStatus(t, ctx, db.Pool, matchSrc, "MATCHED")
	assertTransactionStatus(t, ctx, db.Pool, matchTgt, "MATCHED")
	assertTransactionStatus(t, ctx, db.Pool, unmatchedTx, "UNMATCHED")

	var matchResultCount int
	if err := db.Pool.QueryRow(ctx, `SELECT count(*) FROM match_results WHERE tenant_id = $1`, tenantID).Scan(&matchResultCount); err != nil {
		t.Fatalf("count match_results failed: %v", err)
	}
	if matchResultCount != 1 {
		t.Fatalf("expected 1 match_result row, got %d", matchResultCount)
	}

	if len(breakSink.opened) != 1 {
		t.Fatalf("expected 1 Break opened for the unmatched transaction, got %d: %+v", len(breakSink.opened), breakSink.opened)
	}
	if breakSink.opened[0].TransactionIDs[0] != unmatchedTx {
		t.Errorf("expected the break to reference the unmatched transaction %s, got %+v", unmatchedTx, breakSink.opened[0])
	}
	if !breakSink.opened[0].AmountAtRisk.Equal(decimal.RequireFromString("12.34")) {
		t.Errorf("expected amount at risk 12.34, got %s", breakSink.opened[0].AmountAtRisk)
	}
}

func waitForJobCompletion(t *testing.T, ctx context.Context, pool *pgxpool.Pool, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var pending int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM river_job WHERE state IN ('available', 'running', 'scheduled', 'retryable')`).Scan(&pending); err != nil {
			t.Fatalf("check pending river jobs failed: %v", err)
		}
		if pending == 0 {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	rows, err := pool.Query(ctx, `SELECT id, state, errors::text FROM river_job`)
	if err != nil {
		t.Fatalf("debug query failed: %v", err)
	}
	defer rows.Close()
	for rows.Next() {
		var id int64
		var state string
		var errs string
		if err := rows.Scan(&id, &state, &errs); err != nil {
			t.Fatalf("debug scan failed: %v", err)
		}
		t.Logf("job %d state=%s errors=%s", id, state, errs)
	}
	t.Fatal("timed out waiting for river jobs to complete")
}

func assertTransactionStatus(t *testing.T, ctx context.Context, pool *pgxpool.Pool, id uuid.UUID, want string) {
	t.Helper()
	var got string
	if err := pool.QueryRow(ctx, `SELECT status FROM transactions WHERE id = $1`, id).Scan(&got); err != nil {
		t.Fatalf("query transaction status failed: %v", err)
	}
	if got != want {
		t.Errorf("transaction %s: expected status %q, got %q", id, want, got)
	}
}
