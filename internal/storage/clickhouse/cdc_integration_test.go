package clickhouse_test

import (
	"context"
	"database/sql"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/koriebruh/Jengine/internal/storage/clickhouse"
)

const (
	localRedpandaBroker  = "localhost:9092"
	localKafkaConnectURL = "http://localhost:8083"
	localClickHouseAddr  = "localhost:9004"
	localSuperuserDevDSN = "postgres://jengine:jengine_dev@localhost:5432/jengine?sslmode=disable"
)

// requireLocalCDCStack mirrors internal/platform/outbox's own
// requireLocalStreamingStack (task 18) - Debezium/Kafka Connect and
// ClickHouse are heavy processes not worth spinning up fresh per test
// run; this test targets the real local dev stack (`make dev-up
// streaming-up register-connectors` + `docker compose up -d clickhouse`
// + `scripts/apply-clickhouse-ddl.sh`), skipping if it isn't up.
func requireLocalCDCStack(t *testing.T) {
	t.Helper()
	conn, err := net.DialTimeout("tcp", localRedpandaBroker, 2*time.Second)
	if err != nil {
		t.Skipf("local Redpanda not reachable at %s (run `make dev-up`): %v", localRedpandaBroker, err)
	}
	_ = conn.Close()

	httpClient := http.Client{Timeout: 3 * time.Second}
	resp, err := httpClient.Get(localKafkaConnectURL + "/connectors/jengine-cdc-transactions-connector/status")
	if err != nil || resp.StatusCode != http.StatusOK {
		t.Skipf("jengine-cdc-transactions-connector not running (run `make streaming-up register-connectors`): %v", err)
	}
	_ = resp.Body.Close()

	chConn, err := net.DialTimeout("tcp", localClickHouseAddr, 2*time.Second)
	if err != nil {
		t.Skipf("local ClickHouse not reachable at %s (run `docker compose up -d clickhouse` + scripts/apply-clickhouse-ddl.sh): %v", localClickHouseAddr, err)
	}
	_ = chConn.Close()
}

// TestTransactionCDC_AppearsInClickHouse proves plans/task/core/22's
// core DoD requirement: a Transaction row inserted into Postgres
// appears in ClickHouse's transactions_local within a bounded time,
// via the real Debezium -> Kafka -> ClickHouse Kafka Engine -> MV
// pipeline - not a mock, the actual configured connectors and DDL.
func TestTransactionCDC_AppearsInClickHouse(t *testing.T) {
	requireLocalCDCStack(t)

	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
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
	idempotencyKey := uuid.New()
	_, err = pgPool.Exec(ctx, `
		INSERT INTO transactions (id, tenant_id, account_id, external_ref, amount, currency, base_amount, value_date, booking_date, side, source_mode, status, ingestion_idempotency_key)
		VALUES ($1, $2, $3, 'cdc-integration-test', 42.42, 'USD', 42.42, current_date, current_date, 'CREDIT', 'BATCH', 'UNMATCHED', $4)`,
		txnID, tenantID, accountID, idempotencyKey,
	)
	if err != nil {
		t.Fatalf("insert test transaction: %v", err)
	}
	t.Cleanup(func() {
		_, _ = pgPool.Exec(context.Background(), `DELETE FROM transactions WHERE id = $1`, txnID)
	})

	chConn, err := clickhouse.NewClient(ctx, localClickHouseAddr, "jengine", "default", "")
	if err != nil {
		t.Fatalf("connect to ClickHouse: %v", err)
	}
	defer func() { _ = chConn.Close() }()

	// CDC pipeline latency in this local dev stack was observed (during
	// this task's own manual verification) to normally land within a
	// few seconds; this deadline is generous rather than tight.
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		// amount is Decimal(20,4) - clickhouse-go v2 can't scan that
		// into *string directly ("converting Decimal to *string is
		// unsupported"), so it's cast to Float64 in the query itself.
		row := chConn.QueryRow(ctx, `SELECT toFloat64(amount), status FROM transactions_local FINAL WHERE id = ?`, txnID)
		var amount float64
		var status string
		err := row.Scan(&amount, &status)
		if err == nil {
			if status != "UNMATCHED" {
				t.Errorf("expected status UNMATCHED, got %s", status)
			}
			if amount != 42.42 {
				t.Errorf("expected amount 42.42, got %v", amount)
			}
			return
		}
		if err != sql.ErrNoRows {
			t.Fatalf("unexpected query error (not just 'row not found yet'): %v", err)
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatalf("transaction id=%s never appeared in ClickHouse transactions_local within the deadline", txnID)
}
