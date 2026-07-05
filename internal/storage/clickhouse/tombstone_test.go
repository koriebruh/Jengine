package clickhouse_test

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/koriebruh/Jengine/internal/storage/clickhouse"
)

// TestDebeziumUpdateDelete_NoDuplicateOrStaleRows is the DoD's tombstone-
// handling test: an UPDATE must not leave a stale pre-update row behind
// once queried with FINAL, and a DELETE must make the row disappear
// under FINAL entirely - the Common Pitfall this task's own text names
// ("plain MergeTree would accumulate every version as a separate row").
// Requires migrations/0012's REPLICA IDENTITY FULL on transactions (see
// deploy/clickhouse/ddl.sql's own comment on why DEFAULT replica
// identity isn't enough for a correct delete tombstone).
func TestDebeziumUpdateDelete_NoDuplicateOrStaleRows(t *testing.T) {
	requireLocalCDCStack(t)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	pgPool, err := pgxpool.New(ctx, localSuperuserDevDSN)
	if err != nil {
		t.Fatalf("connect to local dev Postgres: %v", err)
	}
	defer pgPool.Close()

	var tenantID, accountID uuid.UUID
	if err := pgPool.QueryRow(ctx, `SELECT id FROM tenants LIMIT 1`).Scan(&tenantID); err != nil {
		t.Skipf("no seeded tenant in local dev DB to test against: %v", err)
	}
	if err := pgPool.QueryRow(ctx, `SELECT id FROM accounts WHERE tenant_id = $1 LIMIT 1`, tenantID).Scan(&accountID); err != nil {
		t.Skipf("no seeded account for tenant %s: %v", tenantID, err)
	}

	txnID := uuid.New()
	_, err = pgPool.Exec(ctx, `
		INSERT INTO transactions (id, tenant_id, account_id, external_ref, amount, currency, base_amount, value_date, booking_date, side, source_mode, status, ingestion_idempotency_key)
		VALUES ($1, $2, $3, 'tombstone-test', 10.00, 'USD', 10.00, current_date, current_date, 'CREDIT', 'BATCH', 'UNMATCHED', $4)`,
		txnID, tenantID, accountID, uuid.New(),
	)
	if err != nil {
		t.Fatalf("insert fixture transaction: %v", err)
	}

	chConn, err := clickhouse.NewClient(ctx, localClickHouseAddr, "jengine", "default", "")
	if err != nil {
		t.Fatalf("connect to ClickHouse: %v", err)
	}
	defer func() { _ = chConn.Close() }()

	waitForStatus := func(want string) {
		t.Helper()
		deadline := time.Now().Add(30 * time.Second)
		for {
			var status string
			err := chConn.QueryRow(ctx, `SELECT status FROM transactions_local FINAL WHERE id = ?`, txnID).Scan(&status)
			if err == nil && status == want {
				return
			}
			if err != nil && err != sql.ErrNoRows {
				t.Fatalf("unexpected query error: %v", err)
			}
			if time.Now().After(deadline) {
				t.Fatalf("transaction never reached status %q within the deadline (last err=%v, last status=%q)", want, err, status)
			}
			time.Sleep(500 * time.Millisecond)
		}
	}

	waitForStatus("UNMATCHED")

	// --- update ---
	if _, err := pgPool.Exec(ctx, `UPDATE transactions SET status = 'MATCHED' WHERE id = $1`, txnID); err != nil {
		t.Fatalf("update fixture transaction: %v", err)
	}
	waitForStatus("MATCHED")

	// FINAL must show exactly one row, at the latest status - not the
	// pre-update UNMATCHED row surviving alongside it.
	var countAfterUpdate uint64
	if err := chConn.QueryRow(ctx, `SELECT count() FROM transactions_local FINAL WHERE id = ?`, txnID).Scan(&countAfterUpdate); err != nil {
		t.Fatalf("count after update failed: %v", err)
	}
	if countAfterUpdate != 1 {
		t.Errorf("expected exactly 1 row under FINAL after update, got %d (stale pre-update row not suppressed)", countAfterUpdate)
	}

	// --- delete ---
	if _, err := pgPool.Exec(ctx, `DELETE FROM transactions WHERE id = $1`, txnID); err != nil {
		t.Fatalf("delete fixture transaction: %v", err)
	}

	deadline := time.Now().Add(30 * time.Second)
	for {
		var count uint64
		err := chConn.QueryRow(ctx, `SELECT count() FROM transactions_local FINAL WHERE id = ?`, txnID).Scan(&count)
		if err != nil {
			t.Fatalf("count after delete failed: %v", err)
		}
		if count == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("row still present under FINAL after delete within the deadline (tombstone not suppressing it - check REPLICA IDENTITY FULL is applied, migrations/0012)")
		}
		time.Sleep(500 * time.Millisecond)
	}
}
