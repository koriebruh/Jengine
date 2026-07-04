// Command ingestion-gateway is a minimal manual-verification entry point
// for plans/task/core/06: runs a fake connector's records through the
// full ingestion pipeline (persist + transactional outbox + Redpanda
// relay) against the local dev stack (`make dev-up`). Real CLI/scheduling
// wiring for actual connectors stays thin until plans/task/core/07 needs
// it - see plans/task/core/06's Definition of Done.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/shopspring/decimal"

	"github.com/koriebruh/Jengine/internal/domain"
	"github.com/koriebruh/Jengine/internal/ingestion"
	"github.com/koriebruh/Jengine/internal/ingestion/connector"
	"github.com/koriebruh/Jengine/internal/ingestion/connector/testconnector"
	"github.com/koriebruh/Jengine/internal/ingestion/kafka"
	"github.com/koriebruh/Jengine/internal/ingestion/pipeline"
	"github.com/koriebruh/Jengine/internal/storage/postgres"
	"github.com/koriebruh/Jengine/internal/tenancy"
)

type passthroughStage struct{ name string }

func (s passthroughStage) Name() string { return s.name }
func (s passthroughStage) Process(ctx context.Context, rec *pipeline.PipelineRecord) (pipeline.StageResult, error) {
	return pipeline.StageContinue, nil
}

func main() {
	if err := run(); err != nil {
		log.Fatalf("ingestion-gateway: %v", err)
	}
}

func run() error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	superuserDSN := envOrDefault("DATABASE_URL", "postgres://jengine:jengine_dev@localhost:5432/jengine?sslmode=disable")
	appDSN := envOrDefault("APP_DATABASE_URL", "postgres://jengine_app:jengine_app_dev@localhost:5432/jengine?sslmode=disable")
	brokers := []string{envOrDefault("REDPANDA_BROKERS", "localhost:9092")}

	superuserPool, err := pgxpool.New(ctx, superuserDSN)
	if err != nil {
		return fmt.Errorf("connect as superuser: %w", err)
	}
	defer superuserPool.Close()

	appPool, err := pgxpool.New(ctx, appDSN)
	if err != nil {
		return fmt.Errorf("connect as jengine_app: %w", err)
	}
	defer appPool.Close()

	tenantID := uuid.New()
	accountID := uuid.New()
	if _, err := superuserPool.Exec(ctx,
		`INSERT INTO tenants (id, name, isolation_tier, region, status) VALUES ($1, 'ingestion-gateway manual verification', 'STANDARD', 'us-east', 'ACTIVE')`,
		tenantID,
	); err != nil {
		return fmt.Errorf("seed tenant: %w", err)
	}
	if _, err := superuserPool.Exec(ctx,
		`INSERT INTO accounts (id, tenant_id, external_account_ref, account_type, currency, name) VALUES ($1, $2, 'ACC-GATEWAY', 'BANK', 'USD', 'Manual Verification Account')`,
		accountID, tenantID,
	); err != nil {
		return fmt.Errorf("seed account: %w", err)
	}

	connectorID := uuid.New()
	topic := fmt.Sprintf("ingestion.raw.%s", tenantID.String()[:8])

	txRepo := postgres.NewTransactionRepo()
	outboxRepo := postgres.NewOutboxRepo(superuserPool)

	persistFn := func(ctx context.Context, rec *pipeline.PipelineRecord) (string, string, []byte, error) {
		tx, err := txRepo.Create(ctx, tenantID, domain.Transaction{
			AccountID:               accountID,
			ExternalRef:             "GATEWAY-001",
			Amount:                  decimal.RequireFromString("100.00"),
			Currency:                "USD",
			BaseAmount:              decimal.RequireFromString("100.00"),
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
			&postgres.PersistEmitStage{Pool: appPool, TenantID: tenantID, Outbox: outboxRepo, Persist: persistFn},
		},
	}

	rec := testconnector.NewRecord(tenantID, connectorID, []byte("manual-verification-payload"))
	conn := testconnector.New([]connector.RawRecord{rec})

	runCtx := tenancy.WithTenant(ctx, tenancy.TenantContext{TenantID: tenantID})
	if err := pl.Run(runCtx, conn, connector.ConnectorConfig{TenantID: tenantID, ConnectorID: connectorID}); err != nil {
		return fmt.Errorf("pipeline run: %w", err)
	}
	log.Printf("persisted 1 transaction for tenant %s, account %s", tenantID, accountID)

	producer, err := kafka.NewProducer(brokers)
	if err != nil {
		return fmt.Errorf("new kafka producer: %w", err)
	}
	defer producer.Close()

	relay := &ingestion.OutboxRelay{Reader: outboxRepo, Producer: producer}
	sent, err := relay.RunOnce(ctx)
	if err != nil {
		return fmt.Errorf("relay run: %w", err)
	}
	log.Printf("relay published %d event(s) to topic %q - verify with: rpk topic consume %s -n 1", sent, topic, topic)

	return nil
}

func envOrDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
