package outbox_test

import (
	"context"
	"encoding/base64"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/koriebruh/Jengine/internal/platform/outbox"
)

const (
	localRedpandaBroker  = "localhost:9092"
	localKafkaConnectURL = "http://localhost:8083"
	localDevDSN          = "postgres://jengine_app:jengine_app_dev@localhost:5432/jengine?sslmode=disable"
)

// requireLocalStreamingStack skips unless the local dev stack's Redpanda
// AND a RUNNING jengine-outbox-connector are both reachable - this test
// automates what plans/task/core/18's manual verification step checked
// by hand (insert a row, `rpk topic consume` it back): it targets the
// actual `make dev-up` + `make streaming-up` + `make register-connectors`
// stack, not a fresh testcontainer (Kafka Connect/Debezium is a heavy
// JVM process; spinning a fresh one per test run is impractical - same
// "target the local dev stack" convention as
// internal/ingestion's requireLocalRedpanda).
func requireLocalStreamingStack(t *testing.T) {
	t.Helper()
	conn, err := net.DialTimeout("tcp", localRedpandaBroker, 2*time.Second)
	if err != nil {
		t.Skipf("local Redpanda not reachable at %s (run `make dev-up`): %v", localRedpandaBroker, err)
	}
	_ = conn.Close()

	httpClient := http.Client{Timeout: 3 * time.Second}
	resp, err := httpClient.Get(localKafkaConnectURL + "/connectors/jengine-outbox-connector/status")
	if err != nil || resp.StatusCode != http.StatusOK {
		t.Skipf("jengine-outbox-connector not running/registered at %s (run `make streaming-up register-connectors`): %v", localKafkaConnectURL, err)
	}
	_ = resp.Body.Close()
}

// TestOutboxToKafka_EndToEnd proves the full outbox pattern
// (plans/task/core/18, plans/docs/06-streaming-architecture.md §7.3): a
// row inserted into outbox_event via outbox.Insert is observed on its
// target Kafka topic within a bounded time, via Debezium's CDC +
// outbox-event-router SMT - not a Go poller, the actual configured
// pipeline.
func TestOutboxToKafka_EndToEnd(t *testing.T) {
	requireLocalStreamingStack(t)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, localDevDSN)
	if err != nil {
		t.Fatalf("connect to local dev Postgres: %v", err)
	}
	defer pool.Close()

	superuserPool, err := pgxpool.New(ctx, "postgres://jengine:jengine_dev@localhost:5432/jengine?sslmode=disable")
	if err != nil {
		t.Fatalf("connect to local dev Postgres (superuser): %v", err)
	}
	defer superuserPool.Close()

	var tenantID uuid.UUID
	if err := superuserPool.QueryRow(ctx, `SELECT id FROM tenants LIMIT 1`).Scan(&tenantID); err != nil {
		t.Skipf("no seeded tenant in local dev DB to test against: %v", err)
	}

	aggregateID := uuid.New()
	testTopic := "normalized.transactions.default"
	payload := []byte("cdc-integration-test-" + aggregateID.String())

	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	if _, err := tx.Exec(ctx, `SELECT set_config('app.current_tenant_id', $1, true)`, tenantID.String()); err != nil {
		t.Fatalf("set_config: %v", err)
	}
	if err := outbox.Insert(ctx, tx, tenantID, outbox.Event{
		AggregateType: "transaction", AggregateID: aggregateID,
		EventType: "transaction.created", Topic: testTopic, Payload: payload,
	}); err != nil {
		t.Fatalf("outbox.Insert: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit: %v", err)
	}
	t.Cleanup(func() {
		_, _ = superuserPool.Exec(context.Background(), `DELETE FROM outbox_event WHERE aggregate_id = $1`, aggregateID)
	})

	client, err := kgo.NewClient(
		kgo.SeedBrokers(localRedpandaBroker),
		kgo.ConsumeTopics(testTopic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AfterMilli(time.Now().Add(-5*time.Second).UnixMilli())),
	)
	if err != nil {
		t.Fatalf("consumer client: %v", err)
	}
	defer client.Close()

	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		fetchCtx, fetchCancel := context.WithTimeout(ctx, 5*time.Second)
		fetches := client.PollFetches(fetchCtx)
		fetchCancel()

		found := false
		fetches.EachRecord(func(rec *kgo.Record) {
			if string(rec.Key) != aggregateID.String() {
				return
			}
			found = true
			// The connector is configured with binary.handling.mode=
			// base64 + a String value.converter (deploy/debezium/
			// outbox-connector.json) - found during this task's own
			// manual verification that Kafka Connect's stock
			// ByteArrayConverter can't handle the ByteBuffer Debezium
			// produces for a bytea column. The wire value is therefore
			// base64 text, not raw bytes.
			decoded, err := base64.StdEncoding.DecodeString(string(rec.Value))
			if err != nil {
				t.Errorf("failed to base64-decode record value: %v", err)
				return
			}
			if string(decoded) != string(payload) {
				t.Errorf("payload mismatch: got %q want %q", decoded, payload)
			}
		})
		if found {
			return
		}
	}
	t.Fatalf("outbox_event row for aggregate_id=%s never appeared on topic %s within the deadline", aggregateID, testTopic)
}
