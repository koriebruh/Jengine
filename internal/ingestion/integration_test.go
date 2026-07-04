package ingestion_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"

	"github.com/koriebruh/Jengine/internal/domain"
	"github.com/koriebruh/Jengine/internal/ingestion"
	"github.com/koriebruh/Jengine/internal/ingestion/connector"
	"github.com/koriebruh/Jengine/internal/ingestion/connector/testconnector"
	"github.com/koriebruh/Jengine/internal/ingestion/kafka"
	"github.com/koriebruh/Jengine/internal/ingestion/pipeline"
	"github.com/koriebruh/Jengine/internal/storage/postgres"
	"github.com/koriebruh/Jengine/internal/tenancy"
	"github.com/koriebruh/Jengine/internal/testutil"

	kgo "github.com/twmb/franz-go/pkg/kgo"
)

const localRedpandaBroker = "localhost:9092"

// requireLocalRedpanda skips the test if the local dev-stack Redpanda
// (plans/task/core/02) isn't reachable - this integration test
// deliberately targets "local Redpanda" per plans/task/core/06's DoD
// wording, not a fresh testcontainer, matching the same target as the
// task's "manual verification via cmd/ingestion-gateway against the
// local dev stack" step.
func requireLocalRedpanda(t *testing.T) {
	t.Helper()
	conn, err := net.DialTimeout("tcp", localRedpandaBroker, 2*time.Second)
	if err != nil {
		t.Skipf("local Redpanda not reachable at %s (run `make dev-up`): %v", localRedpandaBroker, err)
	}
	_ = conn.Close()
}

// passthroughStage is a trivial named stage standing in for the real
// format-parse/field-mapping/normalization/validation/dedup/canonicalization
// stages tasks 07-09 implement - this task's own integration test only
// needs to prove the persist+outbox+relay+Kafka path works end-to-end,
// not real per-format parsing logic (plans/task/core/06 Non-Goals).
type passthroughStage struct{ name string }

func (s passthroughStage) Name() string { return s.name }
func (s passthroughStage) Process(ctx context.Context, rec *pipeline.PipelineRecord) (pipeline.StageResult, error) {
	return pipeline.StageContinue, nil
}

// TestIngestion_FullPipelineToRedpanda proves plans/task/core/06's
// Definition of Done: a fake connector's records flow through the full
// pipeline, end with a Transaction row persisted (via task 05
// repositories) and a corresponding event message readable from the
// ingestion.raw.<tenant_shard> topic - proving the transactional-outbox-
// to-Kafka relay actually works, not just the DB write half.
func TestIngestion_FullPipelineToRedpanda(t *testing.T) {
	if testing.Short() {
		t.Skip("requires Docker - skipped under -short; run make test-integration")
	}
	requireLocalRedpanda(t)

	db := testutil.StartPostgres(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	tenantID := uuid.New()
	_, err := db.Pool.Exec(ctx,
		`INSERT INTO tenants (id, name, isolation_tier, region, status) VALUES ($1, 'Acme', 'STANDARD', 'us-east', 'ACTIVE')`,
		tenantID,
	)
	if err != nil {
		t.Fatalf("seed tenant failed: %v", err)
	}
	accountID := uuid.New()
	_, err = db.Pool.Exec(ctx,
		`INSERT INTO accounts (id, tenant_id, external_account_ref, account_type, currency, name) VALUES ($1, $2, 'ACC-ING', 'BANK', 'USD', 'Ingestion Test Account')`,
		accountID, tenantID,
	)
	if err != nil {
		t.Fatalf("seed account failed: %v", err)
	}

	// This test's own testcontainer Postgres is on a random host port
	// (from db.DSN), so it must run migrations for quarantine_entries/
	// ingestion_outbox too - testutil.StartPostgres already applies every
	// migrations/*.up.sql file, so those tables exist automatically.

	appPool := testutil.AppRolePool(t, ctx, db.DSN)
	defer appPool.Close()

	topic := fmt.Sprintf("ingestion.raw.test-%s", tenantID.String()[:8])
	connectorID := uuid.New()

	txRepo := postgres.NewTransactionRepo()
	outboxRepo := postgres.NewOutboxRepo(db.Pool) // superuser pool - see OutboxRepo doc comment

	persistFn := func(ctx context.Context, rec *pipeline.PipelineRecord) (string, string, []byte, error) {
		tx, err := txRepo.Create(ctx, tenantID, domain.Transaction{
			AccountID:               accountID,
			ExternalRef:             "ING-001",
			Amount:                  decimal.RequireFromString("42.00"),
			Currency:                "USD",
			BaseAmount:              decimal.RequireFromString("42.00"),
			ValueDate:               time.Now(),
			BookingDate:             time.Now(),
			Side:                    domain.TransactionSideCredit,
			SourceMode:              domain.SourceModeBatch,
			IngestionIdempotencyKey: rec.Raw.BatchID.String(),
			Status:                  domain.TransactionStatusUnmatched,
		})
		if err != nil {
			return "", "", nil, err
		}
		payload, _ := json.Marshal(map[string]any{"transaction_id": tx.ID.String(), "tenant_id": tenantID.String()})
		return topic, tx.ID.String(), payload, nil
	}

	pl := &pipeline.Pipeline{
		Stages: []pipeline.Stage{
			passthroughStage{"format_parse"},
			passthroughStage{"field_mapping"},
			passthroughStage{"normalization"},
			passthroughStage{"validation"},
			passthroughStage{"dedup"},
			passthroughStage{"canonicalization"},
			&postgres.PersistEmitStage{
				Pool:     appPool,
				TenantID: tenantID,
				Outbox:   outboxRepo,
				Persist:  persistFn,
			},
		},
	}

	rec := testconnector.NewRecord(tenantID, connectorID, []byte("raw-payload"))
	conn := testconnector.New([]connector.RawRecord{rec})

	// PersistEmitStage's Persist func calls txRepo.Create, which requires
	// a TenantContext in ctx (tenancy.MustTenantFromContext) - this is
	// the "caller wraps ctx before calling WithTx/Run" contract documented
	// on postgres.WithTx and tenancy.WithTenantTx.
	runCtx := tenancy.WithTenant(ctx, tenancy.TenantContext{TenantID: tenantID})

	if err := pl.Run(runCtx, conn, connector.ConnectorConfig{TenantID: tenantID, ConnectorID: connectorID}); err != nil {
		t.Fatalf("pipeline Run failed: %v", err)
	}

	// --- DB half: Transaction row persisted ---
	var txCount int
	if err := db.Pool.QueryRow(ctx, `SELECT count(*) FROM transactions WHERE account_id = $1`, accountID).Scan(&txCount); err != nil {
		t.Fatalf("count query failed: %v", err)
	}
	if txCount != 1 {
		t.Fatalf("expected 1 transaction persisted, got %d", txCount)
	}

	// --- Outbox half: unsent row written in the same transaction ---
	var outboxCount int
	if err := db.Pool.QueryRow(ctx, `SELECT count(*) FROM ingestion_outbox WHERE tenant_id = $1 AND sent_at IS NULL`, tenantID).Scan(&outboxCount); err != nil {
		t.Fatalf("outbox count query failed: %v", err)
	}
	if outboxCount != 1 {
		t.Fatalf("expected 1 unsent outbox row, got %d", outboxCount)
	}

	// --- Relay half: publish to the real local Redpanda and mark sent ---
	producer, err := kafka.NewProducer([]string{localRedpandaBroker})
	if err != nil {
		t.Fatalf("kafka.NewProducer failed: %v", err)
	}
	defer producer.Close()

	relay := &ingestion.OutboxRelay{Reader: outboxRepo, Producer: producer}
	sent, err := relay.RunOnce(ctx)
	if err != nil {
		t.Fatalf("relay.RunOnce failed: %v", err)
	}
	if sent != 1 {
		t.Fatalf("expected relay to publish 1 event, published %d", sent)
	}

	var sentAt *time.Time
	if err := db.Pool.QueryRow(ctx, `SELECT sent_at FROM ingestion_outbox WHERE tenant_id = $1`, tenantID).Scan(&sentAt); err != nil {
		t.Fatalf("sent_at query failed: %v", err)
	}
	if sentAt == nil {
		t.Fatal("expected sent_at to be set after the relay published the event")
	}

	// --- Consume the message back from Redpanda to prove it's really there ---
	consumer, err := kgo.NewClient(
		kgo.SeedBrokers(localRedpandaBroker),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		t.Fatalf("failed to create consumer: %v", err)
	}
	defer consumer.Close()

	fetchCtx, fetchCancel := context.WithTimeout(ctx, 15*time.Second)
	defer fetchCancel()

	found := false
	for !found {
		fetches := consumer.PollFetches(fetchCtx)
		if fetches.IsClientClosed() || fetchCtx.Err() != nil {
			break
		}
		fetches.EachRecord(func(r *kgo.Record) {
			var payload map[string]any
			if err := json.Unmarshal(r.Value, &payload); err == nil {
				if payload["tenant_id"] == tenantID.String() {
					found = true
				}
			}
		})
	}
	if !found {
		t.Fatalf("expected to read the published event back from topic %q, but it was never seen", topic)
	}
}
