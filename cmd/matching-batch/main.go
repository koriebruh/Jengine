// Command matching-batch runs plans/task/core/12's batch matching
// worker: partitions unmatched transactions, runs internal/matching/core.Match
// with rules compiled by internal/matching/rules, and writes results
// back. Default mode ("seed") is the manual-verification flow: seed a
// tenant/two accounts/a rule/a handful of transactions, run one full
// batch pass, and report the resulting match/break outcomes. Pass
// -demo=serve to instead start a long-lived River worker pool that polls
// for new partitions on a schedule and blocks.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"log/slog"
	"os"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"
	"go.opentelemetry.io/otel"

	"github.com/koriebruh/Jengine/internal/audit"
	"github.com/koriebruh/Jengine/internal/cases"
	"github.com/koriebruh/Jengine/internal/matching/batch"
	"github.com/koriebruh/Jengine/internal/matching/rules"
	"github.com/koriebruh/Jengine/internal/platform/observability"
	"github.com/koriebruh/Jengine/internal/storage/postgres"
	"github.com/koriebruh/Jengine/internal/tenancy"
)

func main() {
	demo := flag.String("demo", "seed", "which mode to run: \"seed\" (default; manual-verification flow: seed data, run one batch pass, report outcomes) or \"serve\" (start a long-lived worker pool on a schedule tick, blocks)")
	flag.Parse()

	ctx := context.Background()
	obsCfg := observability.Config{
		ServiceName: "matching-batch", ServiceVersion: "dev", Environment: envOrDefault("ENVIRONMENT", "dev"),
		OTLPEndpoint: envOrDefault("OTEL_EXPORTER_OTLP_ENDPOINT", "localhost:4318"),
		MetricsAddr:  envOrDefault("METRICS_ADDR", ":9092"),
	}
	slog.SetDefault(observability.NewLogger(obsCfg))

	shutdownTracer, err := observability.InitTracerProvider(ctx, obsCfg)
	if err != nil {
		log.Fatalf("matching-batch: init tracer provider: %v", err)
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = shutdownTracer(shutdownCtx)
	}()

	shutdownMeter, err := observability.InitMeterProvider(ctx, obsCfg)
	if err != nil {
		log.Fatalf("matching-batch: init meter provider: %v", err)
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = shutdownMeter(shutdownCtx)
	}()

	metrics, err := observability.NewMetrics(otel.Meter("matching-batch"))
	if err != nil {
		log.Fatalf("matching-batch: new metrics: %v", err)
	}

	switch *demo {
	case "serve":
		err = runServe(metrics)
	default:
		err = runSeedDemo(metrics)
	}
	if err != nil {
		log.Fatalf("matching-batch: %v", err)
	}
}

// instrumentedWorker wraps batch.PartitionWorker with
// observability.WrapBatchJob (plans/task/core/16) - a fresh root span
// plus golden-signal metrics per job, without internal/matching/batch
// itself needing to import internal/platform/observability (see
// batch.NewRiverClient's doc comment on why it accepts the
// river.Worker[PartitionJobArgs] interface rather than the concrete
// worker type).
type instrumentedWorker struct {
	river.WorkerDefaults[batch.PartitionJobArgs]
	inner   *batch.PartitionWorker
	metrics *observability.Metrics
}

func (w *instrumentedWorker) Work(ctx context.Context, job *river.Job[batch.PartitionJobArgs]) error {
	return observability.WrapBatchJob(ctx, "matching-batch", "process_partition", w.metrics, 0, func(ctx context.Context) error {
		return w.inner.Work(ctx, job)
	})
}

func envOrDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func newTxRunner(pool *pgxpool.Pool) batch.TxRunner {
	return func(ctx context.Context, tenantID uuid.UUID, fn func(ctx context.Context) error) error {
		return postgres.WithTx(tenancy.WithTenant(ctx, tenancy.TenantContext{TenantID: tenantID}), pool, tenantID, fn)
	}
}

// newBreakSink wires the real plans/task/core/13 BreakSink implementation
// (cases.BreakSinkAdapter over cases.PostgresLifecycleService) - this
// binary was originally built and manually verified against a logging
// stand-in before task 13 existed (its own Prerequisites explicitly allow
// that sequencing), now replaced with the real thing.
func newBreakSink(appPool *pgxpool.Pool) *cases.BreakSinkAdapter {
	lifecycle := cases.NewPostgresLifecycleService(
		cases.TxRunner(newTxRunner(appPool)),
		postgres.NewCaseRepo(),
		audit.NewPostgresWriter(),
	)
	return cases.NewBreakSinkAdapter(lifecycle)
}

func newWorkerDeps(appPool *pgxpool.Pool) batch.WorkerDeps {
	return batch.WorkerDeps{
		TxRunner:     newTxRunner(appPool),
		Transactions: postgres.NewTransactionRepo(),
		MatchResults: postgres.NewMatchResultRepo(),
		MatchRules:   postgres.NewMatchRuleRepo(),
		Registry:     rules.DefaultRegistry(),
		BreakSink:    newBreakSink(appPool),
	}
}

// runSeedDemo is the manual-verification target from plans/task/core/12's
// Definition of Done: seed a small tenant, two accounts, a handful of
// transactions, and one active rule; run the worker end-to-end via a real
// River client; report the resulting match/break outcomes.
func runSeedDemo(metrics *observability.Metrics) error {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
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

	if err := batch.EnsureRiverSchema(ctx, superuserPool); err != nil {
		return fmt.Errorf("ensure river schema: %w", err)
	}

	tenantID := uuid.New()
	if _, err := superuserPool.Exec(ctx,
		`INSERT INTO tenants (id, name, isolation_tier, region, status) VALUES ($1, 'matching-batch-demo', 'STANDARD', 'us-east', 'ACTIVE')`,
		tenantID,
	); err != nil {
		return fmt.Errorf("seed tenant: %w", err)
	}

	accountA, accountB := uuid.New(), uuid.New()
	for i, id := range []uuid.UUID{accountA, accountB} {
		if _, err := superuserPool.Exec(ctx,
			`INSERT INTO accounts (id, tenant_id, external_account_ref, account_type, currency, name) VALUES ($1, $2, $3, 'BANK', 'USD', $4)`,
			id, tenantID, id.String(), fmt.Sprintf("Demo Account %d", i+1),
		); err != nil {
			return fmt.Errorf("seed account: %w", err)
		}
	}

	day := time.Now().UTC().Truncate(24 * time.Hour)
	insertTx := func(accountID uuid.UUID, ref, amount string) uuid.UUID {
		id := uuid.New()
		_, err := superuserPool.Exec(ctx,
			`INSERT INTO transactions (id, tenant_id, account_id, external_ref, amount, currency, base_amount, value_date, booking_date, side, source_mode, ingestion_idempotency_key, status)
			 VALUES ($1, $2, $3, $4, $5, 'USD', $5, $6, $6, 'DEBIT', 'BATCH', $7, 'UNMATCHED')`,
			id, tenantID, accountID, ref, amount, day, id.String(),
		)
		if err != nil {
			log.Printf("matching-batch: seed transaction failed: %v", err)
		}
		return id
	}

	matchSrc := insertTx(accountA, "REF-DEMO-001", "500.00")
	matchTgt := insertTx(accountB, "REF-DEMO-001", "500.00")
	unmatchedTx := insertTx(accountA, "REF-DEMO-NOMATCH", "12.34")

	ruleSpec := rules.RuleSpec{}
	ruleSpec.Rule.Name = "matching-batch demo rule"
	ruleSpec.Rule.Version = 1
	ruleSpec.Rule.MatchCardinality = "ONE_TO_ONE"
	ruleSpec.Rule.Keys = []rules.KeySpec{{Field: "currency", Tolerance: rules.ToleranceYAML{Type: "exact"}}}
	ruleSpec.Rule.Scoring = []rules.ScoringSpec{{Field: "reference", Method: "exact", Weight: 1.0}}
	ruleSpec.Rule.Thresholds = rules.ThresholdSpec{AutoMatch: 0.9, Suggest: 0.5}
	ruleSpec.Rule.Execution = rules.ExecutionSpec{Priority: 1}
	ruleSpecJSON, err := json.Marshal(ruleSpec)
	if err != nil {
		return fmt.Errorf("marshal rule spec: %w", err)
	}
	if _, err := superuserPool.Exec(ctx,
		`INSERT INTO match_rules (id, tenant_id, name, version, status, rule_spec, match_type, source_account_id, target_account_id, priority, auto_match_threshold, created_by)
		 VALUES ($1, $2, 'matching-batch demo rule', 1, 'ACTIVE', $3, 'COMPOSITE', $4, $5, 1, 0.9, 'matching-batch')`,
		uuid.New(), tenantID, ruleSpecJSON, accountA, accountB,
	); err != nil {
		return fmt.Errorf("seed match rule: %w", err)
	}

	worker := &instrumentedWorker{inner: &batch.PartitionWorker{Deps: newWorkerDeps(appPool)}, metrics: metrics}
	riverClient, err := batch.NewRiverClient(superuserPool, worker, 0)
	if err != nil {
		return fmt.Errorf("new river client: %w", err)
	}
	if err := riverClient.Start(ctx); err != nil {
		return fmt.Errorf("start river client: %w", err)
	}
	defer func() { _ = riverClient.Stop(ctx) }()

	partitions, err := batch.EnumeratePartitions(ctx, superuserPool, time.Time{})
	if err != nil {
		return fmt.Errorf("enumerate partitions: %w", err)
	}
	log.Printf("matching-batch: enumerated %d partition(s) for tenant %s", len(partitions), tenantID)
	if err := batch.EnqueuePartitions(ctx, riverClient, partitions); err != nil {
		return fmt.Errorf("enqueue partitions: %w", err)
	}

	if err := waitForJobs(ctx, superuserPool, 30*time.Second); err != nil {
		return err
	}

	statusOf := func(id uuid.UUID) string {
		var s string
		if err := superuserPool.QueryRow(ctx, `SELECT status FROM transactions WHERE id = $1`, id).Scan(&s); err != nil {
			return "ERROR:" + err.Error()
		}
		return s
	}
	var matchResultCount int
	if err := superuserPool.QueryRow(ctx, `SELECT count(*) FROM match_results WHERE tenant_id = $1`, tenantID).Scan(&matchResultCount); err != nil {
		return fmt.Errorf("count match_results: %w", err)
	}
	var breakStatus, breakID string
	if err := superuserPool.QueryRow(ctx,
		`SELECT id, status FROM cases WHERE tenant_id = $1 AND $2 = ANY(related_transaction_ids)`,
		tenantID, unmatchedTx,
	).Scan(&breakID, &breakStatus); err != nil {
		breakStatus = "ERROR:" + err.Error()
	}

	log.Printf("matching-batch: SEED DEMO RESULTS - tenant=%s", tenantID)
	log.Printf("  matched pair: source=%s status=%s, target=%s status=%s", matchSrc, statusOf(matchSrc), matchTgt, statusOf(matchTgt))
	log.Printf("  unmatched:    id=%s status=%s -> break id=%s status=%s (plans/task/core/13's real BreakSink)", unmatchedTx, statusOf(unmatchedTx), breakID, breakStatus)
	log.Printf("  match_results written: %d", matchResultCount)
	return nil
}

func waitForJobs(ctx context.Context, pool *pgxpool.Pool, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var pending int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM river_job WHERE state IN ('available', 'running', 'scheduled', 'retryable')`).Scan(&pending); err != nil {
			return fmt.Errorf("check pending river jobs: %w", err)
		}
		if pending == 0 {
			return nil
		}
		time.Sleep(200 * time.Millisecond)
	}
	return fmt.Errorf("timed out after %s waiting for river jobs to complete", timeout)
}

// runServe starts a long-lived worker pool: River processes jobs as they
// arrive, plus a scheduled tick (the "safety-net catch-all for streaming-
// sourced or otherwise statement-less transactions" plans/task/core/12
// Implementation Notes describes) that periodically calls
// EnumeratePartitions and enqueues anything new. Blocks until
// interrupted.
func runServe(metrics *observability.Metrics) error {
	ctx, cancel := context.WithTimeout(context.Background(), 24*time.Hour)
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

	if err := batch.EnsureRiverSchema(ctx, superuserPool); err != nil {
		return fmt.Errorf("ensure river schema: %w", err)
	}

	worker := &instrumentedWorker{inner: &batch.PartitionWorker{Deps: newWorkerDeps(appPool)}, metrics: metrics}
	riverClient, err := batch.NewRiverClient(superuserPool, worker, 0)
	if err != nil {
		return fmt.Errorf("new river client: %w", err)
	}
	if err := riverClient.Start(ctx); err != nil {
		return fmt.Errorf("start river client: %w", err)
	}
	defer func() { _ = riverClient.Stop(ctx) }()

	tickInterval := 5 * time.Minute
	ticker := time.NewTicker(tickInterval)
	defer ticker.Stop()

	watermark := time.Time{}
	log.Printf("matching-batch: serving - polling for new partitions every %s, Ctrl+C to stop", tickInterval)
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			tickStart := time.Now()
			partitions, err := batch.EnumeratePartitions(ctx, superuserPool, watermark)
			if err != nil {
				log.Printf("matching-batch: enumerate partitions: %v", err)
				continue
			}
			if err := batch.EnqueuePartitions(ctx, riverClient, partitions); err != nil {
				log.Printf("matching-batch: enqueue partitions: %v", err)
				continue
			}
			log.Printf("matching-batch: tick enqueued %d partition(s)", len(partitions))
			watermark = tickStart
		}
	}
}
