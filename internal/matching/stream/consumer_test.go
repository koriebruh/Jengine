package stream_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/koriebruh/Jengine/internal/domain"
	"github.com/koriebruh/Jengine/internal/matching/rules"
	"github.com/koriebruh/Jengine/internal/matching/stream"
	"github.com/koriebruh/Jengine/internal/storage/postgres"
	"github.com/koriebruh/Jengine/internal/tenancy"
	"github.com/koriebruh/Jengine/internal/testutil"
)

func TestConsumer_Process_StreamingMatchIsProvisional(t *testing.T) {
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

	// Must be recent (not a fixed historical date like the batch
	// worker's own fixtures use) - the streaming pool's trim-on-write
	// compares ValueDate against real time.Now(), so a stale fixture
	// date gets evicted by its own Add() call before ever being
	// queryable. Found via this test's own first run.
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

	firstTxID := insertTx(accountA, "REF-STREAM-001", "250.00")
	secondTxID := insertTx(accountB, "REF-STREAM-001", "250.00")

	ruleSpec := rules.RuleSpec{}
	ruleSpec.Rule.Name = "Streaming test rule"
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
		 VALUES ($1, $2, 'Streaming test rule', 1, 'ACTIVE', $3, 'COMPOSITE', $4, $5, 1, 0.9, 'test')`,
		ruleID, tenantID, ruleSpecJSON, accountA, accountB,
	); err != nil {
		t.Fatalf("seed match rule failed: %v", err)
	}

	appPool := testutil.AppRolePool(t, ctx, db.DSN)
	defer appPool.Close()

	txRunner := func(ctx context.Context, tid uuid.UUID, fn func(ctx context.Context) error) error {
		return postgres.WithTx(tenancy.WithTenant(ctx, tenancy.TenantContext{TenantID: tid}), appPool, tid, fn)
	}

	pool := stream.NewRedisCandidatePool(rdb.Client, "test-consumer", 7*24*time.Hour)
	consumer := &stream.Consumer{
		Deps: stream.WorkerDeps{
			TxRunner:     txRunner,
			Transactions: postgres.NewTransactionRepo(),
			MatchResults: postgres.NewMatchResultRepo(),
			MatchRules:   postgres.NewMatchRuleRepo(),
			Registry:     rules.DefaultRegistry(),
			Pool:         pool,
		},
	}

	var firstTxn, secondTxn domain.Transaction
	if err := txRunner(ctx, tenantID, func(ctx context.Context) error {
		var err error
		if firstTxn, err = postgres.NewTransactionRepo().GetByID(ctx, tenantID, firstTxID); err != nil {
			return err
		}
		if secondTxn, err = postgres.NewTransactionRepo().GetByID(ctx, tenantID, secondTxID); err != nil {
			return err
		}
		return nil
	}); err != nil {
		t.Fatalf("load transactions failed: %v", err)
	}

	// First event: nothing in the pool yet, gets added.
	if err := consumer.Process(ctx, tenantID, firstTxn); err != nil {
		t.Fatalf("Process (first) failed: %v", err)
	}
	// Second event: should match against the first, now pooled.
	if err := consumer.Process(ctx, tenantID, secondTxn); err != nil {
		t.Fatalf("Process (second) failed: %v", err)
	}

	var status string
	var confidence float64
	if err := db.Pool.QueryRow(ctx,
		`SELECT status, confidence_score FROM match_results WHERE tenant_id = $1`, tenantID,
	).Scan(&status, &confidence); err != nil {
		t.Fatalf("query match_results failed: %v", err)
	}
	if status != string(domain.MatchResultStatusAutoMatchedStreaming) {
		t.Errorf("expected status %s (PROVISIONAL), got %s", domain.MatchResultStatusAutoMatchedStreaming, status)
	}
	if confidence < 0.9 {
		t.Errorf("expected high confidence auto-match, got %f", confidence)
	}

	// The transaction's own status is untouched by the streaming path -
	// only the batch/streaming reconciliation job (task 19's other
	// half) or the batch pass itself ever finalizes Transaction.status;
	// a provisional streaming match must not jump ahead of that.
	var txStatus string
	if err := db.Pool.QueryRow(ctx, `SELECT status FROM transactions WHERE id = $1`, firstTxID).Scan(&txStatus); err != nil {
		t.Fatalf("query transaction status failed: %v", err)
	}
	if txStatus != "UNMATCHED" {
		t.Errorf("expected transaction status to remain UNMATCHED after a provisional streaming match, got %s", txStatus)
	}
}
