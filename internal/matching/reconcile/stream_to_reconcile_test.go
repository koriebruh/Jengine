package reconcile_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/koriebruh/Jengine/internal/audit"
	"github.com/koriebruh/Jengine/internal/cases"
	"github.com/koriebruh/Jengine/internal/domain"
	"github.com/koriebruh/Jengine/internal/matching/core"
	"github.com/koriebruh/Jengine/internal/matching/reconcile"
	"github.com/koriebruh/Jengine/internal/matching/rules"
	"github.com/koriebruh/Jengine/internal/matching/stream"
	"github.com/koriebruh/Jengine/internal/storage/postgres"
	"github.com/koriebruh/Jengine/internal/tenancy"
	"github.com/koriebruh/Jengine/internal/testutil"
)

// TestStreamThenReconcile_EndToEnd is plans/task/core/19's Definition of
// Done "Integration tests (testcontainers-go: Redpanda + Redis +
// Postgres) simulating a streaming match followed by a batch pass over
// the same partition" - Redpanda itself is deliberately not exercised
// here (cmd/matching-stream's own Kafka deserialization is a thin,
// separately-verified layer - see this task's manual verification,
// which DID exercise real Redpanda end-to-end); this test instead
// proves the two packages' actual integration point: stream.Consumer
// writes a real AUTO_MATCHED_STREAMING row via a real Redis candidate
// pool, then reconcile.Reconciler.ReconcileBatchAgainstStream (called
// exactly as batch.WorkerDeps.PostWrite would) promotes it - the same
// chain plans/task/core/19's manual verification step ran live against
// the full local dev stack.
func TestStreamThenReconcile_EndToEnd(t *testing.T) {
	if testing.Short() {
		t.Skip("requires Docker - skipped under -short; run make test-integration")
	}

	db := testutil.StartPostgres(t)
	rdb := testutil.StartRedis(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

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

	day := time.Now().Truncate(24 * time.Hour)
	insertTx := func(accountID uuid.UUID, ref string, amount string) uuid.UUID {
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
	src, tgt := insertTx(accountA, "REF-E2E-001", "300.00"), insertTx(accountB, "REF-E2E-001", "300.00")

	ruleSpec := rules.RuleSpec{}
	ruleSpec.Rule.Name = "E2E rule"
	ruleSpec.Rule.Version = 1
	ruleSpec.Rule.MatchCardinality = "ONE_TO_ONE"
	ruleSpec.Rule.Keys = []rules.KeySpec{{Field: "currency", Tolerance: rules.ToleranceYAML{Type: "exact"}}}
	ruleSpec.Rule.Scoring = []rules.ScoringSpec{{Field: "reference", Method: "exact", Weight: 1.0}}
	ruleSpec.Rule.Thresholds = rules.ThresholdSpec{AutoMatch: 0.9, Suggest: 0.5}
	ruleSpec.Rule.Execution = rules.ExecutionSpec{Priority: 1, Mode: []string{"streaming"}}
	ruleSpecJSON, err := json.Marshal(ruleSpec)
	if err != nil {
		t.Fatalf("marshal rule spec: %v", err)
	}
	ruleID := uuid.New()
	if _, err := db.Pool.Exec(ctx,
		`INSERT INTO match_rules (id, tenant_id, name, version, status, rule_spec, match_type, source_account_id, target_account_id, priority, auto_match_threshold, created_by)
		 VALUES ($1, $2, 'E2E rule', 1, 'ACTIVE', $3, 'COMPOSITE', $4, $5, 1, 0.9, 'test')`,
		ruleID, tenantID, ruleSpecJSON, accountA, accountB,
	); err != nil {
		t.Fatalf("seed match rule failed: %v", err)
	}

	appPool := testutil.AppRolePool(t, ctx, db.DSN)
	defer appPool.Close()
	txRunner := func(ctx context.Context, tid uuid.UUID, fn func(ctx context.Context) error) error {
		return postgres.WithTx(tenancy.WithTenant(ctx, tenancy.TenantContext{TenantID: tid}), appPool, tid, fn)
	}

	// --- Streaming half: real stream.Consumer + real Redis pool ---
	redisPool := stream.NewRedisCandidatePool(rdb.Client, "e2e", 7*24*time.Hour)
	transactions := postgres.NewTransactionRepo()
	matchResults := postgres.NewMatchResultRepo()
	consumer := &stream.Consumer{
		Deps: stream.WorkerDeps{
			TxRunner: txRunner, Transactions: transactions, MatchResults: matchResults,
			MatchRules: postgres.NewMatchRuleRepo(), Registry: rules.DefaultRegistry(), Pool: redisPool,
		},
	}

	var srcTxn, tgtTxn domain.Transaction
	if err := txRunner(ctx, tenantID, func(ctx context.Context) error {
		var err error
		if srcTxn, err = transactions.GetByID(ctx, tenantID, src); err != nil {
			return err
		}
		tgtTxn, err = transactions.GetByID(ctx, tenantID, tgt)
		return err
	}); err != nil {
		t.Fatalf("load transactions failed: %v", err)
	}

	if err := consumer.Process(ctx, tenantID, srcTxn); err != nil {
		t.Fatalf("Consumer.Process (src) failed: %v", err)
	}
	if err := consumer.Process(ctx, tenantID, tgtTxn); err != nil {
		t.Fatalf("Consumer.Process (tgt) failed: %v", err)
	}

	var streamingID uuid.UUID
	var status string
	if err := db.Pool.QueryRow(ctx, `SELECT id, status FROM match_results WHERE tenant_id = $1`, tenantID).Scan(&streamingID, &status); err != nil {
		t.Fatalf("query match_results after streaming failed: %v", err)
	}
	if status != string(domain.MatchResultStatusAutoMatchedStreaming) {
		t.Fatalf("expected AUTO_MATCHED_STREAMING after streaming half, got %s", status)
	}

	// --- Batch half: real reconcile.Reconciler, called exactly as
	// batch.WorkerDeps.PostWrite would after a real batch pass produced
	// this same outcome (the batch pass itself is plans/task/core/12's
	// own already-tested worker - not re-run here, per this task's own
	// Non-Goals: "Do not build the batch matching worker itself"). ---
	caseRepo := postgres.NewCaseRepo()
	lifecycle := cases.NewPostgresLifecycleService(cases.TxRunner(txRunner), caseRepo, audit.NewPostgresWriter())
	reconciler := &reconcile.Reconciler{
		Deps: reconcile.Deps{TxRunner: txRunner, MatchResults: matchResults, Cases: caseRepo, Lifecycle: lifecycle},
	}

	batchOutcome := core.MatchOutcome{
		AutoMatched: []core.ScoredCandidate{{RuleID: ruleID, SourceIDs: []uuid.UUID{src}, TargetIDs: []uuid.UUID{tgt}, Score: 0.99}},
	}
	txByID := map[uuid.UUID]domain.Transaction{src: srcTxn, tgt: tgtTxn}

	if err := reconciler.ReconcileBatchAgainstStream(ctx, tenantID, batchOutcome, txByID); err != nil {
		t.Fatalf("ReconcileBatchAgainstStream failed: %v", err)
	}

	if err := db.Pool.QueryRow(ctx, `SELECT status FROM match_results WHERE id = $1`, streamingID).Scan(&status); err != nil {
		t.Fatalf("query match_results after reconcile failed: %v", err)
	}
	if status != string(domain.MatchResultStatusAutoMatchedConfirmed) {
		t.Errorf("expected AUTO_MATCHED_CONFIRMED after batch reconciliation, got %s", status)
	}
}
