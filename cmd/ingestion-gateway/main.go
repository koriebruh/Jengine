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
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/robfig/cron/v3"
	"github.com/shopspring/decimal"

	"github.com/redis/go-redis/v9"

	"github.com/koriebruh/Jengine/internal/domain"
	"github.com/koriebruh/Jengine/internal/ingestion"
	"github.com/koriebruh/Jengine/internal/ingestion/connector"
	"github.com/koriebruh/Jengine/internal/ingestion/connector/csvupload"
	"github.com/koriebruh/Jengine/internal/ingestion/connector/kafkasource"
	"github.com/koriebruh/Jengine/internal/ingestion/connector/sftp"
	"github.com/koriebruh/Jengine/internal/ingestion/connector/testconnector"
	"github.com/koriebruh/Jengine/internal/ingestion/connector/webhookreceiver"
	"github.com/koriebruh/Jengine/internal/ingestion/dedup"
	"github.com/koriebruh/Jengine/internal/ingestion/kafka"
	"github.com/koriebruh/Jengine/internal/ingestion/mapping"
	"github.com/koriebruh/Jengine/internal/ingestion/objectstore"
	"github.com/koriebruh/Jengine/internal/ingestion/pipeline"
	"github.com/koriebruh/Jengine/internal/ingestion/validation"
	"github.com/koriebruh/Jengine/internal/storage/postgres"
	"github.com/koriebruh/Jengine/internal/tenancy"
)

func main() {
	demo := flag.String("demo", "seed", "which demo to run: \"seed\" (SFTP+MT940 sample file, default; used by `make seed`), \"fake\" (plans/task/core/06's fake-connector pipeline demo), \"poll\" (start the cron dispatcher and block, polling the seed SFTP connector on a schedule), or \"malformed-csv\" (plans/task/core/09 manual verification: a CSV row missing currency is quarantined, not crashed on or silently dropped)")
	flag.Parse()

	var err error
	switch *demo {
	case "fake":
		err = runFakeConnectorDemo()
	case "poll":
		err = runPollDemo()
	case "malformed-csv":
		err = runMalformedCSVDemo()
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

// newTxRunner builds a closure wrapping postgres.WithTx, satisfying every
// package-local TxRunner type in this codebase (csvupload.TxRunner,
// sftp.TxRunner, mapping.TxRunner) - they all share the same underlying
// function signature, so one closure works for all of them.
func newTxRunner(pool *pgxpool.Pool) func(ctx context.Context, tenantID uuid.UUID, fn func(ctx context.Context) error) error {
	return func(ctx context.Context, tenantID uuid.UUID, fn func(ctx context.Context) error) error {
		return postgres.WithTx(tenancy.WithTenant(ctx, tenancy.TenantContext{TenantID: tenantID}), pool, tenantID, fn)
	}
}

// sharedWebhookReceiver is the one webhookreceiver.Connector instance
// every tenant's "webhook" registration shares (plans/task/core/18) -
// unlike the pull-based connectors below, it's stateful (holds a
// per-connector-ID config map + the single inbound records channel
// ServeHTTP enqueues onto), so registry.New("webhook", cfg) must return
// the SAME instance each time, not a fresh one. Exported so cmd/main can
// also mount its ServeHTTP on the HTTP server; see webhookreceiver's own
// package doc for why this diverges from every other connector's shape.
func newRegistry(pool *pgxpool.Pool, statements *postgres.StatementRepo, secrets sftp.SecretResolver, store csvupload.ObjectStore) (*connector.Registry, *webhookreceiver.Connector) {
	reg := connector.NewRegistry()
	txRunner := newTxRunner(pool)

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

	// webhookreceiver.SecretResolver/kafkasource.SecretResolver are
	// structurally identical to sftp.SecretResolver (same Resolve
	// signature) - Go's structural interface satisfaction means the
	// same secrets value passed into this function already satisfies
	// both, no adapter needed (plans/task/core/18).
	webhook := webhookreceiver.New(secrets)
	_ = reg.Register("webhook", func(cfg connector.ConnectorConfig) (connector.SourceConnector, error) {
		return webhook, nil
	})
	_ = reg.Register("kafka_source", func(cfg connector.ConnectorConfig) (connector.SourceConnector, error) {
		return kafkasource.New(secrets), nil
	})

	return reg, webhook
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

// formatParseStage implements pipeline stage 2 (Format Parse) for
// connectors whose Payload is already structured JSON (per-record, e.g.
// internal/ingestion/connector/sftp's {"field_61": ..., "field_86": ...}
// - the real MT940 field-tag parsing already happened inside the
// connector's Fetch, task 07's job; this stage is just the trivial
// JSON-to-map bridge plans/task/core/08's MappingEngine needs for its
// ParsedFields input.
type formatParseStage struct{}

func (formatParseStage) Name() string { return "format_parse" }
func (formatParseStage) Process(ctx context.Context, rec *pipeline.PipelineRecord) (pipeline.StageResult, error) {
	if err := json.Unmarshal(rec.Raw.Payload, &rec.ParsedFields); err != nil {
		return pipeline.StageQuarantine, fmt.Errorf("format_parse: %w", err)
	}
	return pipeline.StageContinue, nil
}

// canonicalizePersistFn implements stage 7 (Canonicalization) inline with
// stage 8 (Persist+Emit): builds a domain.Transaction directly from
// rec.Normalized (plans/task/core/08's NormalizationStage output),
// exactly the "shaped closely enough" handoff plans/task/core/08's own
// Implementation Notes describe.
func canonicalizePersistFn(txRepo *postgres.TransactionRepo, tenantID, accountID uuid.UUID) postgres.PersistFunc {
	return func(ctx context.Context, rec *pipeline.PipelineRecord) (string, string, []byte, error) {
		n := rec.Normalized
		tx, err := txRepo.Create(ctx, tenantID, domain.Transaction{
			AccountID:               accountID,
			StatementID:             &rec.Raw.BatchID,
			ExternalRef:             n.ExternalRef,
			Amount:                  n.Amount,
			Currency:                n.Currency,
			BaseAmount:              n.BaseAmount,
			FXRateToBase:            &n.FXRateToBase,
			ValueDate:               n.ValueDate,
			BookingDate:             n.BookingDate,
			Description:             n.Description,
			Side:                    n.Side,
			SourceMode:              domain.SourceModeBatch,
			IngestionIdempotencyKey: rec.Raw.BatchID.String() + "-" + n.ExternalRef,
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
	accountRepo := postgres.NewAccountRepo()
	fxRateRepo := postgres.NewFXRateRepo()
	mappingSpecRepo := postgres.NewMappingSpecRepo()
	outboxRepo := postgres.NewOutboxRepo(superuserPool)

	connectorID := uuid.New()
	if _, err := superuserPool.Exec(ctx,
		`INSERT INTO connectors (id, tenant_id, type, status) VALUES ($1, $2, 'sftp_mt940', 'ACTIVE')`,
		connectorID, tenantID,
	); err != nil {
		return fmt.Errorf("seed connector: %w", err)
	}

	// Seed the real mt940_default.yaml mapping spec (plans/task/core/08),
	// compiled to JSON for the mapping_specs.spec jsonb column - not the
	// stub/ad-hoc parsing task 07's own seed flow used before this task.
	mt940Spec, err := mapping.ParseSpecYAML(mapping.MT940DefaultSpecYAML)
	if err != nil {
		return fmt.Errorf("parse mt940 default mapping spec: %w", err)
	}
	mt940SpecJSON, err := json.Marshal(mt940Spec)
	if err != nil {
		return fmt.Errorf("marshal mt940 default mapping spec: %w", err)
	}
	if _, err := superuserPool.Exec(ctx,
		`INSERT INTO mapping_specs (tenant_id, source_format, version, status, spec) VALUES ($1, 'MT940', 1, 'ACTIVE', $2)`,
		tenantID, mt940SpecJSON,
	); err != nil {
		return fmt.Errorf("seed mapping spec: %w", err)
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

	reg, _ := newRegistry(appPool, statementRepo, sftp.EnvSecretResolver{}, store)
	sftpConn, err := reg.New("sftp_mt940", cfg)
	if err != nil {
		return fmt.Errorf("registry.New(sftp_mt940): %w", err)
	}

	redisAddr := envOrDefault("REDIS_ADDR", "localhost:6379")
	rdb := redis.NewClient(&redis.Options{Addr: redisAddr})
	defer func() { _ = rdb.Close() }()
	bloom := dedup.NewRedisBloomFilter(rdb, "dedup:bloom", 10_000, 0.01)
	dedupRepo := postgres.NewIngestionDedupRepo()
	quarantineSink := postgres.NewPipelineQuarantineSink(superuserPool)

	pl := &pipeline.Pipeline{
		Stages: []pipeline.Stage{
			formatParseStage{},
			mapping.NewEngine(mappingSpecRepo, newTxRunner(appPool)),
			&mapping.NormalizationStage{
				TenantID: tenantID, AccountID: accountID,
				Accounts: accountRepo, FXRates: fxRateRepo,
				TxRunner: newTxRunner(appPool),
			},
			&validation.ValidationStage{AccountID: accountID},
			&dedup.DedupStage{
				TenantID: tenantID, ConnectorID: connectorID,
				Bloom: bloom, Transactions: txRepo, Dedup: dedupRepo,
				TxRunner: newTxRunner(appPool),
			},
			&postgres.PersistEmitStage{
				Pool: appPool, TenantID: tenantID, Outbox: outboxRepo,
				Persist: canonicalizePersistFn(txRepo, tenantID, accountID),
			},
		},
		Quarantine: quarantineSink,
		OnRecordProcessed: func(o pipeline.RecordOutcome) {
			log.Printf("seed: record outcome: quarantined=%v dropped=%v stages=%v err=%v", o.Quarantined, o.Dropped, o.StageOrder, o.Err)
		},
	}

	runCtx := tenancy.WithTenant(ctx, tenancy.TenantContext{TenantID: tenantID})
	if err := pl.Run(runCtx, sftpConn, cfg); err != nil {
		return fmt.Errorf("pipeline run: %w", err)
	}

	var txCount int
	var sampleAmount, sampleCurrency string
	if err := superuserPool.QueryRow(ctx,
		`SELECT count(*), max(amount::text), max(currency) FROM transactions WHERE account_id = $1`,
		accountID,
	).Scan(&txCount, &sampleAmount, &sampleCurrency); err != nil {
		return fmt.Errorf("count transactions: %w", err)
	}
	log.Printf("seed: tenant=%s account=%s - %d transaction row(s) persisted from SFTP+MT940 sample file (mapped+normalized via plans/task/core/08, e.g. amount=%s currency=%s)",
		tenantID, accountID, txCount, sampleAmount, sampleCurrency)
	if txCount == 0 {
		return fmt.Errorf("seed produced 0 transaction rows - check the sftp service is up (docker compose up -d sftp) and scripts/seed-data/incoming/sample.sta is mounted")
	}
	return nil
}

// inMemoryObjectStore is a trivial csvupload.ObjectStore for
// runMalformedCSVDemo - no MinIO round trip needed to demonstrate the
// quarantine path.
type inMemoryObjectStore struct{ files map[string][]byte }

func (s *inMemoryObjectStore) Get(ctx context.Context, bucket, key string) ([]byte, error) {
	return s.files[bucket+"/"+key], nil
}

// runMalformedCSVDemo is plans/task/core/09's manual verification: a CSV
// row missing the required currency column must be quarantined (a real,
// queryable quarantine_entries row), never crash the process and never
// be silently dropped.
func runMalformedCSVDemo() error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	superuserDSN := envOrDefault("DATABASE_URL", "postgres://jengine:jengine_dev@localhost:5432/jengine?sslmode=disable")
	appDSN := envOrDefault("APP_DATABASE_URL", "postgres://jengine_app:jengine_app_dev@localhost:5432/jengine?sslmode=disable")

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
	connectorID := uuid.New()
	if _, err := superuserPool.Exec(ctx,
		`INSERT INTO tenants (id, name, isolation_tier, region, status) VALUES ($1, 'malformed-csv-demo', 'STANDARD', 'us-east', 'ACTIVE')`,
		tenantID,
	); err != nil {
		return fmt.Errorf("seed tenant: %w", err)
	}
	if _, err := superuserPool.Exec(ctx,
		`INSERT INTO accounts (id, tenant_id, external_account_ref, account_type, currency, name) VALUES ($1, $2, 'ACC-MALFORMED', 'BANK', 'USD', 'Malformed CSV Demo Account')`,
		accountID, tenantID,
	); err != nil {
		return fmt.Errorf("seed account: %w", err)
	}
	if _, err := superuserPool.Exec(ctx,
		`INSERT INTO connectors (id, tenant_id, type, status) VALUES ($1, $2, 'csv', 'ACTIVE')`,
		connectorID, tenantID,
	); err != nil {
		return fmt.Errorf("seed connector: %w", err)
	}

	csvSpec, err := mapping.ParseSpecYAML(mapping.CSVDefaultSpecYAML)
	if err != nil {
		return fmt.Errorf("parse csv default mapping spec: %w", err)
	}
	csvSpecJSON, err := json.Marshal(csvSpec)
	if err != nil {
		return fmt.Errorf("marshal csv default mapping spec: %w", err)
	}
	if _, err := superuserPool.Exec(ctx,
		`INSERT INTO mapping_specs (tenant_id, source_format, version, status, spec) VALUES ($1, 'CSV', 1, 'ACTIVE', $2)`,
		tenantID, csvSpecJSON,
	); err != nil {
		return fmt.Errorf("seed mapping spec: %w", err)
	}

	statementRepo := postgres.NewStatementRepo()
	txRepo := postgres.NewTransactionRepo()
	accountRepo := postgres.NewAccountRepo()
	fxRateRepo := postgres.NewFXRateRepo()
	mappingSpecRepo := postgres.NewMappingSpecRepo()
	outboxRepo := postgres.NewOutboxRepo(superuserPool)
	quarantineSink := postgres.NewPipelineQuarantineSink(superuserPool)

	// Deliberately missing the "currency" column csv_default.yaml's
	// mapping spec requires.
	const malformedCSV = "amount,value_date,description\n100.00,2024-01-15,missing currency column\n"
	store := &inMemoryObjectStore{files: map[string][]byte{"demo/malformed.csv": []byte(malformedCSV)}}

	csvConn := csvupload.New(store, statementRepo, newTxRunner(appPool))

	cfg := connector.ConnectorConfig{
		TenantID: tenantID, ConnectorID: connectorID, Type: "csv",
		Settings: mustJSON(map[string]any{
			"bucket": "demo", "object_key": "malformed.csv", "format": "csv",
			"account_id": accountID, "duplicate_policy": "correction",
		}),
	}

	quarantinedCount := 0
	pl := &pipeline.Pipeline{
		Stages: []pipeline.Stage{
			formatParseStage{},
			mapping.NewEngine(mappingSpecRepo, newTxRunner(appPool)),
			&mapping.NormalizationStage{
				TenantID: tenantID, AccountID: accountID,
				Accounts: accountRepo, FXRates: fxRateRepo, TxRunner: newTxRunner(appPool),
			},
			&validation.ValidationStage{AccountID: accountID},
			&postgres.PersistEmitStage{
				Pool: appPool, TenantID: tenantID, Outbox: outboxRepo,
				Persist: canonicalizePersistFn(txRepo, tenantID, accountID),
			},
		},
		Quarantine: quarantineSink,
		OnRecordProcessed: func(o pipeline.RecordOutcome) {
			if o.Quarantined {
				quarantinedCount++
			}
			log.Printf("malformed-csv: record outcome: quarantined=%v stages=%v err=%v", o.Quarantined, o.StageOrder, o.Err)
		},
	}

	runCtx := tenancy.WithTenant(ctx, tenancy.TenantContext{TenantID: tenantID})
	if err := pl.Run(runCtx, csvConn, cfg); err != nil {
		return fmt.Errorf("pipeline run: %w", err)
	}

	if quarantinedCount == 0 {
		return fmt.Errorf("expected the malformed row to be quarantined, but no record was reported as quarantined")
	}

	var reason string
	if err := superuserPool.QueryRow(ctx,
		`SELECT reason FROM quarantine_entries WHERE tenant_id = $1 ORDER BY occurred_at DESC LIMIT 1`,
		tenantID,
	).Scan(&reason); err != nil {
		return fmt.Errorf("expected a queryable quarantine_entries row, query failed: %w", err)
	}
	log.Printf("malformed-csv: SUCCESS - malformed row was quarantined, not crashed on, not silently dropped. reason=%q", reason)
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
	accountRepo := postgres.NewAccountRepo()
	fxRateRepo := postgres.NewFXRateRepo()
	mappingSpecRepo := postgres.NewMappingSpecRepo()
	outboxRepo := postgres.NewOutboxRepo(superuserPool)

	store, err := objectstore.NewMinIOStore(minioEndpoint, minioAccessKey, minioSecretKey, false)
	if err != nil {
		return fmt.Errorf("new object store: %w", err)
	}
	reg, _ := newRegistry(appPool, statementRepo, sftp.EnvSecretResolver{}, store)

	connectorID := uuid.New()
	if _, err := superuserPool.Exec(ctx,
		`INSERT INTO connectors (id, tenant_id, type, status) VALUES ($1, $2, 'sftp_mt940', 'ACTIVE')`,
		connectorID, tenantID,
	); err != nil {
		return fmt.Errorf("seed connector: %w", err)
	}

	mt940Spec, err := mapping.ParseSpecYAML(mapping.MT940DefaultSpecYAML)
	if err != nil {
		return fmt.Errorf("parse mt940 default mapping spec: %w", err)
	}
	mt940SpecJSON, err := json.Marshal(mt940Spec)
	if err != nil {
		return fmt.Errorf("marshal mt940 default mapping spec: %w", err)
	}
	if _, err := superuserPool.Exec(ctx,
		`INSERT INTO mapping_specs (tenant_id, source_format, version, status, spec) VALUES ($1, 'MT940', 1, 'ACTIVE', $2)`,
		tenantID, mt940SpecJSON,
	); err != nil {
		return fmt.Errorf("seed mapping spec: %w", err)
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
			formatParseStage{},
			mapping.NewEngine(mappingSpecRepo, newTxRunner(appPool)),
			&mapping.NormalizationStage{
				TenantID: tenantID, AccountID: accountID,
				Accounts: accountRepo, FXRates: fxRateRepo,
				TxRunner: newTxRunner(appPool),
			},
			passthroughStage{"validation"},
			passthroughStage{"dedup"},
			&postgres.PersistEmitStage{
				Pool: appPool, TenantID: tenantID, Outbox: outboxRepo,
				Persist: canonicalizePersistFn(txRepo, tenantID, accountID),
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
