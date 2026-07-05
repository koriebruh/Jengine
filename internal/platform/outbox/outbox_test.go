package outbox_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/koriebruh/Jengine/internal/platform/outbox"
	"github.com/koriebruh/Jengine/internal/testutil"
)

func TestInsert_WritesWithinCallerTransaction(t *testing.T) {
	if testing.Short() {
		t.Skip("requires Docker - skipped under -short; run make test-integration")
	}

	db := testutil.StartPostgres(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	tenantID := uuid.New()
	if _, err := db.Pool.Exec(ctx,
		`INSERT INTO tenants (id, name, isolation_tier, region, status) VALUES ($1, 'Acme', 'STANDARD', 'us-east', 'ACTIVE')`,
		tenantID,
	); err != nil {
		t.Fatalf("seed tenant failed: %v", err)
	}

	appPool := testutil.AppRolePool(t, ctx, db.DSN)
	defer appPool.Close()

	aggregateID := uuid.New()
	tx, err := appPool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin tx failed: %v", err)
	}
	if _, err := tx.Exec(ctx, `SELECT set_config('app.current_tenant_id', $1, true)`, tenantID.String()); err != nil {
		t.Fatalf("set_config failed: %v", err)
	}

	if err := outbox.Insert(ctx, tx, tenantID, outbox.Event{
		AggregateType: "transaction", AggregateID: aggregateID,
		EventType: "transaction.created", Topic: "normalized.transactions.default",
		Payload: []byte("fake-serialized-protobuf"),
	}); err != nil {
		t.Fatalf("Insert failed: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit failed: %v", err)
	}

	var gotAggregateType, gotTopic string
	var gotPayload []byte
	if err := db.Pool.QueryRow(ctx,
		`SELECT aggregate_type, topic, payload FROM outbox_event WHERE tenant_id = $1 AND aggregate_id = $2`,
		tenantID, aggregateID,
	).Scan(&gotAggregateType, &gotTopic, &gotPayload); err != nil {
		t.Fatalf("query outbox_event failed: %v", err)
	}
	if gotAggregateType != "transaction" || gotTopic != "normalized.transactions.default" || string(gotPayload) != "fake-serialized-protobuf" {
		t.Errorf("unexpected row: aggregate_type=%q topic=%q payload=%q", gotAggregateType, gotTopic, gotPayload)
	}
}

func TestInsert_RolledBackTransactionLeavesNoRow(t *testing.T) {
	if testing.Short() {
		t.Skip("requires Docker - skipped under -short; run make test-integration")
	}

	db := testutil.StartPostgres(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	tenantID := uuid.New()
	if _, err := db.Pool.Exec(ctx,
		`INSERT INTO tenants (id, name, isolation_tier, region, status) VALUES ($1, 'Acme', 'STANDARD', 'us-east', 'ACTIVE')`,
		tenantID,
	); err != nil {
		t.Fatalf("seed tenant failed: %v", err)
	}

	appPool := testutil.AppRolePool(t, ctx, db.DSN)
	defer appPool.Close()

	aggregateID := uuid.New()
	tx, err := appPool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin tx failed: %v", err)
	}
	if _, err := tx.Exec(ctx, `SELECT set_config('app.current_tenant_id', $1, true)`, tenantID.String()); err != nil {
		t.Fatalf("set_config failed: %v", err)
	}
	if err := outbox.Insert(ctx, tx, tenantID, outbox.Event{
		AggregateType: "transaction", AggregateID: aggregateID,
		EventType: "transaction.created", Topic: "normalized.transactions.default",
		Payload: []byte("fake"),
	}); err != nil {
		t.Fatalf("Insert failed: %v", err)
	}
	// Simulate the paired domain-state change failing - roll back
	// instead of committing. Proves the outbox write is genuinely
	// atomic with whatever it's paired with, not a separate operation.
	if err := tx.Rollback(ctx); err != nil {
		t.Fatalf("rollback failed: %v", err)
	}

	var count int
	if err := db.Pool.QueryRow(ctx, `SELECT count(*) FROM outbox_event WHERE aggregate_id = $1`, aggregateID).Scan(&count); err != nil {
		t.Fatalf("count outbox_event failed: %v", err)
	}
	if count != 0 {
		t.Errorf("expected 0 rows after rollback, got %d", count)
	}
}
