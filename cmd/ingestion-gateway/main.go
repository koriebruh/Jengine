// Command ingestion-gateway registers plans/task/core/07's connectors
// against the registry and runs `make seed`'s SFTP+MT940 sample-file
// flow by default. Pass -demo=fake to instead run plans/task/core/06's
// fake-connector pipeline demo against the local dev stack. Real
// CLI/scheduling wiring for production connector polling stays thin
// (a single cron dispatcher, see runCronDispatcher) until later tasks
// need more.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/robfig/cron/v3"
	"github.com/shopspring/decimal"

	"github.com/koriebruh/Jengine/internal/domain"
	"github.com/koriebruh/Jengine/internal/ingestion"
	"github.com/koriebruh/Jengine/internal/ingestion/connector"
	"github.com/koriebruh/Jengine/internal/ingestion/connector/csvupload"
	"github.com/koriebruh/Jengine/internal/ingestion/connector/sftp"
	"github.com/koriebruh/Jengine/internal/ingestion/connector/testconnector"
	"github.com/koriebruh/Jengine/internal/ingestion/kafka"
	"github.com/koriebruh/Jengine/internal/ingestion/objectstore"
	"github.com/koriebruh/Jengine/internal/ingestion/parsers/mt940"
	"github.com/koriebruh/Jengine/internal/ingestion/pipeline"
	"github.com/koriebruh/Jengine/internal/storage/postgres"
	"github.com/koriebruh/Jengine/internal/tenancy"
)

func main() {
	demo := flag.String("demo", "seed", "which demo to run: \"seed\" (SFTP+MT940 sample file, default; used by `make seed`), \"fake\" (plans/task/core/06's fake-connector pipeline demo), or \"poll\" (start the cron dispatcher and block, polling the seed SFTP connector on a schedule)")
	flag.Parse()

	var err error
	switch *demo {
	case "fake":
		err = runFakeConnectorDemo()
	case "poll":
		err = runPollDemo()
	default:
		err = runSFTPMT940Seed()
	}
	if err != nil {
		log.Fatalf("ingestion-gateway: %v", err)
	}
}

func envOrDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// newRegistry registers every plans/task/core/07 connector type -
// callers construct instances via registry.New("csv"|"sftp_mt940", cfg).
func newRegistry(pool *pgxpool.Pool, statements *postgres.StatementRepo, secrets sftp.SecretResolver, store csvupload.ObjectStore) *connector.Registry {
	reg := connector.NewRegistry()

	txRunner := func(ctx context.Context, tenantID uuid.UUID, fn func(ctx context.Context) error) error {
		return postgres.WithTx(tenancy.WithTenant(ctx, tenancy.TenantContext{TenantID: tenantID}), pool, tenantID, fn)
	}

	_ = reg.Register("csv", func(cfg connector.ConnectorConfig) (connector.SourceConnector, error) {
		return csvupload.New(store, statements, txRunner), nil
	})
	// Modeled as one composed "sftp_mt940" type (transport=SFTP,
	// format=MT940 always for this fixture flow) per plans/task/core/07
	// Implementation Notes - transport and parser remain separately
	// testable/reusable Go packages underneath.
	_ = reg.Register("sftp_mt940", func(cfg connector.ConnectorConfig) (connector.SourceConnector, error) {
		return sftp.New(txRunner, secrets, statements), nil
	})

	return reg
}

// runCronDispatcher is the "simple cron-based dispatcher" plans/task/core/07
// explicitly scopes this task to (not a distributed job queue) -
// invokes registry.New(...).Fetch(...) per due connector, running each
// result through pl. Exported shape only; not started by either demo mode
// above, since neither needs live polling - real callers wire this into
// their own long-running process.
func runCronDispatcher(reg *connector.Registry, jobs map[string]connector.ConnectorConfig, pl *pipeline.Pipeline) *cron.Cron {
	c := cron.New()
	for connType, cfg := range jobs {
		connType, cfg := connType, cfg
		schedule := cfg.Schedule
		if schedule == "" {
			continue
		}
		_, _ = c.AddFunc(schedule, func() {
			conn, err := reg.New(connType, cfg)
			if err != nil {
				log.Printf("ingestion-gateway: cron: registry.New(%s): %v", connType, err)
				return
			}
			ctx := tenancy.WithTenant(context.Background(), tenancy.TenantContext{TenantID: cfg.TenantID})
			if err := pl.Run(ctx, conn, cfg); err != nil {
				log.Printf("ingestion-gateway: cron: pipeline run for %s: %v", connType, err)
			}
		})
	}
	c.Start()
	return c
}

type passthroughStage struct{ name string }

func (s passthroughStage) Name() string { return s.name }
func (s passthroughStage) Process(ctx context.Context, rec *pipeline.PipelineRecord) (pipeline.StageResult, error) {
	return pipeline.StageContinue, nil
}

// mt940RecordPayload mirrors the JSON shape internal/ingestion/connector/sftp
// emits per record: {"field_61": mt940.Field61, "field_86": mt940.Field86}.
type mt940RecordPayload struct {
	Field61 mt940.Field61 `json:"field_61"`
	Field86 mt940.Field86 `json:"field_86"`
}

// mt940PersistFn is the minimal stand-in mapping plans/task/core/06's own
// DoD anticipated ("use a minimal internal test mapping if needed") since
// plans/task/core/08's real mapping DSL doesn't exist yet - it does just
// enough of field_61/field_86 -> domain.Transaction to prove the SFTP+
// MT940 path produces correct rows end to end, per plans/task/core/07's
// own Definition of Done.
func mt940PersistFn(txRepo *postgres.TransactionRepo, tenantID, accountID uuid.UUID) postgres.PersistFunc {
	return func(ctx context.Context, rec *pipeline.PipelineRecord) (string, string, []byte, error) {
		var p mt940RecordPayload
		if err := json.Unmarshal(rec.Raw.Payload, &p); err != nil {
			return "", "", nil, fmt.Errorf("unmarshal mt940 payload: %w", err)
		}

		amount, err := decimal.NewFromString(strings.Replace(p.Field61.Amount, ",", ".", 1))
		if err != nil {
			return "", "", nil, fmt.Errorf("parse amount %q: %w", p.Field61.Amount, err)
		}

		side := domain.TransactionSideCredit
		if strings.HasPrefix(p.Field61.DebitCreditMark, "D") {
			side = domain.TransactionSideDebit
		}

		valueDate, err := time.Parse("060102", p.Field61.ValueDate)
		if err != nil {
			return "", "", nil, fmt.Errorf("parse value_date %q: %w", p.Field61.ValueDate, err)
		}

		tx, err := txRepo.Create(ctx, tenantID, domain.Transaction{
			AccountID:               accountID,
			StatementID:             &rec.Raw.BatchID,
			ExternalRef:             p.Field61.CustomerRef,
			Amount:                  amount,
			Currency:                p.Field61.Currency,
			BaseAmount:              amount,
			ValueDate:               valueDate,
			BookingDate:             valueDate,
			Description:             p.Field86.Narrative,
			Side:                    side,
			SourceMode:              domain.SourceModeBatch,
			IngestionIdempotencyKey: rec.Raw.BatchID.String() + "-" + p.Field61.CustomerRef,
			Status:                  domain.TransactionStatusUnmatched,
		})
		if err != nil {
			return "", "", nil, err
		}

		payload, _ := json.Marshal(map[string]any{"transaction_id": tx.ID.String(), "tenant_id": tenantID.String()})
		topic := fmt.Sprintf("ingestion.raw.%s", tenantID.String()[:8])
		return topic, tx.ID.String(), payload, nil
	}
}

func runSFTPMT940Seed() error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	superuserDSN := envOrDefault("DATABASE_URL", "postgres://jengine:jengine_dev@localhost:5432/jengine?sslmode=disable")
	appDSN := envOrDefault("APP_DATABASE_URL", "postgres://jengine_app:jengine_app_dev@localhost:5432/jengine?sslmode=disable")
	sftpHost := envOrDefault("SFTP_HOST", "localhost:2222")
	sftpUser := envOrDefault("SFTP_USER", "jengine")

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
		`INSERT INTO tenants (id, name, isolation_tier, region, status) VALUES ($1, 'seed', 'STANDARD', 'us-east', 'ACTIVE')`,
		tenantID,
	); err != nil {
		return fmt.Errorf("seed tenant: %w", err)
	}
	if _, err := superuserPool.Exec(ctx,
		`INSERT INTO accounts (id, tenant_id, external_account_ref, account_type, currency, name) VALUES ($1, $2, '1234567890', 'BANK', 'EUR', 'Seed Account')`,
		accountID, tenantID,
	); err != nil {
		return fmt.Errorf("seed account: %w", err)
	}

	statementRepo := postgres.NewStatementRepo()
	txRepo := postgres.NewTransactionRepo()
	outboxRepo := postgres.NewOutboxRepo(superuserPool)

	connectorID := uuid.New()
	if _, err := superuserPool.Exec(ctx,
		`INSERT INTO connectors (id, tenant_id, type, status) VALUES ($1, $2, 'sftp_mt940', 'ACTIVE')`,
		connectorID, tenantID,
	); err != nil {
		return fmt.Errorf("seed connector: %w", err)
	}

	cfg := connector.ConnectorConfig{
		TenantID: tenantID, ConnectorID: connectorID, Type: "sftp_mt940",
		Settings: mustJSON(map[string]any{
			"host": sftpHost, "username": sftpUser,
			"auth":             map[string]any{"type": "password", "vault_path_ref": "secret/sftp/dev/password"},
			"remote_dir":       "/incoming",
			"account_id":       accountID,
			"parse_format":     "MT940",
			"dialect":          "generic",
			"duplicate_policy": "correction", // seeding is re-runnable
		}),
	}

	minioEndpoint := envOrDefault("MINIO_ENDPOINT", "localhost:9000")
	minioAccessKey := envOrDefault("MINIO_ACCESS_KEY", "jengine")
	minioSecretKey := envOrDefault("MINIO_SECRET_KEY", "jengine_dev_secret")
	store, err := objectstore.NewMinIOStore(minioEndpoint, minioAccessKey, minioSecretKey, false)
	if err != nil {
		return fmt.Errorf("new object store: %w", err)
	}

	reg := newRegistry(appPool, statementRepo, sftp.EnvSecretResolver{}, store)
	sftpConn, err := reg.New("sftp_mt940", cfg)
	if err != nil {
		return fmt.Errorf("registry.New(sftp_mt940): %w", err)
	}

	pl := &pipeline.Pipeline{
		Stages: []pipeline.Stage{
			passthroughStage{"field_mapping"},
			passthroughStage{"normalization"},
			passthroughStage{"validation"},
			passthroughStage{"dedup"},
			passthroughStage{"canonicalization"},
			&postgres.PersistEmitStage{
				Pool: appPool, TenantID: tenantID, Outbox: outboxRepo,
				Persist: mt940PersistFn(txRepo, tenantID, accountID),
			},
		},
	}

	runCtx := tenancy.WithTenant(ctx, tenancy.TenantContext{TenantID: tenantID})
	if err := pl.Run(runCtx, sftpConn, cfg); err != nil {
		return fmt.Errorf("pipeline run: %w", err)
	}

	var txCount int
	if err := superuserPool.QueryRow(ctx, `SELECT count(*) FROM transactions WHERE account_id = $1`, accountID).Scan(&txCount); err != nil {
		return fmt.Errorf("count transactions: %w", err)
	}
	log.Printf("seed: tenant=%s account=%s - %d transaction row(s) persisted from SFTP+MT940 sample file", tenantID, accountID, txCount)
	if txCount == 0 {
		return fmt.Errorf("seed produced 0 transaction rows - check the sftp service is up (docker compose up -d sftp) and scripts/seed-data/incoming/sample.sta is mounted")
	}
	return nil
}

// runPollDemo starts runCronDispatcher against the same SFTP+MT940
// connector runSFTPMT940Seed uses (polling every 30s instead of running
// once) and blocks until interrupted - demonstrates the "simple cron-
// based dispatcher" plans/task/core/07 scopes this task to, as real,
// runnable code rather than an unwired function.
func runPollDemo() error {
	ctx, cancel := context.WithTimeout(context.Background(), 24*time.Hour)
	defer cancel()

	superuserDSN := envOrDefault("DATABASE_URL", "postgres://jengine:jengine_dev@localhost:5432/jengine?sslmode=disable")
	appDSN := envOrDefault("APP_DATABASE_URL", "postgres://jengine_app:jengine_app_dev@localhost:5432/jengine?sslmode=disable")
	sftpHost := envOrDefault("SFTP_HOST", "localhost:2222")
	sftpUser := envOrDefault("SFTP_USER", "jengine")
	minioEndpoint := envOrDefault("MINIO_ENDPOINT", "localhost:9000")
	minioAccessKey := envOrDefault("MINIO_ACCESS_KEY", "jengine")
	minioSecretKey := envOrDefault("MINIO_SECRET_KEY", "jengine_dev_secret")

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
		`INSERT INTO tenants (id, name, isolation_tier, region, status) VALUES ($1, 'poll-demo', 'STANDARD', 'us-east', 'ACTIVE')`,
		tenantID,
	); err != nil {
		return fmt.Errorf("seed tenant: %w", err)
	}
	if _, err := superuserPool.Exec(ctx,
		`INSERT INTO accounts (id, tenant_id, external_account_ref, account_type, currency, name) VALUES ($1, $2, '1234567890', 'BANK', 'EUR', 'Poll Demo Account')`,
		accountID, tenantID,
	); err != nil {
		return fmt.Errorf("seed account: %w", err)
	}

	statementRepo := postgres.NewStatementRepo()
	txRepo := postgres.NewTransactionRepo()
	outboxRepo := postgres.NewOutboxRepo(superuserPool)

	store, err := objectstore.NewMinIOStore(minioEndpoint, minioAccessKey, minioSecretKey, false)
	if err != nil {
		return fmt.Errorf("new object store: %w", err)
	}
	reg := newRegistry(appPool, statementRepo, sftp.EnvSecretResolver{}, store)

	connectorID := uuid.New()
	if _, err := superuserPool.Exec(ctx,
		`INSERT INTO connectors (id, tenant_id, type, status) VALUES ($1, $2, 'sftp_mt940', 'ACTIVE')`,
		connectorID, tenantID,
	); err != nil {
		return fmt.Errorf("seed connector: %w", err)
	}

	cfg := connector.ConnectorConfig{
		TenantID: tenantID, ConnectorID: connectorID, Type: "sftp_mt940", Schedule: "@every 30s",
		Settings: mustJSON(map[string]any{
			"host": sftpHost, "username": sftpUser,
			"auth":             map[string]any{"type": "password", "vault_path_ref": "secret/sftp/dev/password"},
			"remote_dir":       "/incoming",
			"account_id":       accountID,
			"parse_format":     "MT940",
			"dialect":          "generic",
			"duplicate_policy": "correction",
		}),
	}

	pl := &pipeline.Pipeline{
		Stages: []pipeline.Stage{
			passthroughStage{"field_mapping"},
			passthroughStage{"normalization"},
			passthroughStage{"validation"},
			passthroughStage{"dedup"},
			passthroughStage{"canonicalization"},
			&postgres.PersistEmitStage{
				Pool: appPool, TenantID: tenantID, Outbox: outboxRepo,
				Persist: mt940PersistFn(txRepo, tenantID, accountID),
			},
		},
	}

	c := runCronDispatcher(reg, map[string]connector.ConnectorConfig{"sftp_mt940": cfg}, pl)
	defer c.Stop()

	log.Printf("poll: cron dispatcher started for tenant %s, account %s (every 30s) - Ctrl+C to stop", tenantID, accountID)
	<-ctx.Done()
	return nil
}

func mustJSON(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return b
}

// runFakeConnectorDemo is plans/task/core/06's original manual-
// verification wiring, kept available via -demo=fake for regression
// reference.
func runFakeConnectorDemo() error {
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
