// Command matching-stream runs plans/task/core/19's streaming matching
// worker: consumes normalized.transactions.default (task 18), matches
// each incoming transaction against a bounded Redis candidate pool using
// internal/matching/core.Match (the SAME library cmd/matching-batch
// uses, unmodified), and writes provisional AUTO_MATCHED_STREAMING
// results. Default mode ("serve") is the long-running consumer. Pass
// -demo=publish-test-event to publish one synthetic TransactionEvent for
// manual verification (plans/task/core/19 Definition of Done: "publish
// a synthetic streaming event, observe a provisional
// AUTO_MATCHED_STREAMING result").
package main

import (
	"context"
	"flag"
	"hash/fnv"
	"log"
	"log/slog"
	"os"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
	"google.golang.org/protobuf/proto"

	jenginev1 "github.com/koriebruh/Jengine/gen/go/jengine/v1"
	"github.com/koriebruh/Jengine/internal/domain"
	"github.com/koriebruh/Jengine/internal/matching/rules"
	"github.com/koriebruh/Jengine/internal/matching/stream"
	"github.com/koriebruh/Jengine/internal/platform/observability"
	"github.com/koriebruh/Jengine/internal/storage/postgres"
	"github.com/koriebruh/Jengine/internal/tenancy"
)

const (
	streamTopic   = "normalized.transactions.default"
	consumerGroup = "matching-stream"
	// numWorkers is the keyed-worker pool size (plans/task/core/19
	// Implementation Notes: "serialize processing per (tenant_id,
	// account_id) pair... e.g. hash to N keyed worker goroutines").
	// Small, fixed number for MVP - real autoscaling is horizontal
	// (more matching-stream replicas via KEDA,
	// deploy/helm/matching-stream), not more goroutines within one
	// process.
	numWorkers = 8
	// candidatePoolWindow bounds the rolling window (plans/task/
	// core/19 Implementation Notes example: "last 7 days").
	candidatePoolWindow = 7 * 24 * time.Hour
)

func envOrDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func main() {
	demo := flag.String("demo", "serve", "\"serve\" (default; long-running consumer) or \"publish-test-event\" (manual verification helper)")
	flag.Parse()

	ctx := context.Background()
	brokers := []string{envOrDefault("REDPANDA_BROKER", "localhost:9092")}

	if *demo == "publish-test-event" {
		if err := publishTestEvent(ctx, brokers); err != nil {
			log.Fatalf("matching-stream: publish-test-event: %v", err)
		}
		return
	}

	obsCfg := observability.Config{
		ServiceName: "matching-stream", ServiceVersion: "dev", Environment: envOrDefault("ENVIRONMENT", "dev"),
		OTLPEndpoint: envOrDefault("OTEL_EXPORTER_OTLP_ENDPOINT", "localhost:4318"),
		MetricsAddr:  envOrDefault("METRICS_ADDR", ":9093"),
	}
	slog.SetDefault(observability.NewLogger(obsCfg))

	shutdownTracer, err := observability.InitTracerProvider(ctx, obsCfg)
	if err != nil {
		log.Fatalf("matching-stream: init tracer provider: %v", err)
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = shutdownTracer(shutdownCtx)
	}()

	shutdownMeter, err := observability.InitMeterProvider(ctx, obsCfg)
	if err != nil {
		log.Fatalf("matching-stream: init meter provider: %v", err)
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = shutdownMeter(shutdownCtx)
	}()

	appDSN := envOrDefault("APP_DATABASE_URL", "postgres://jengine_app:jengine_app_dev@localhost:5432/jengine?sslmode=disable")
	appPool, err := pgxpool.New(ctx, appDSN)
	if err != nil {
		log.Fatalf("matching-stream: connect as jengine_app: %v", err)
	}
	defer appPool.Close()

	redisAddr := envOrDefault("REDIS_ADDR", "localhost:6379")
	rdb := redis.NewClient(&redis.Options{Addr: redisAddr})
	defer func() { _ = rdb.Close() }()

	pool := stream.NewRedisCandidatePool(rdb, "matching-stream", candidatePoolWindow)

	txRunner := func(ctx context.Context, tenantID uuid.UUID, fn func(ctx context.Context) error) error {
		return postgres.WithTx(tenancy.WithTenant(ctx, tenancy.TenantContext{TenantID: tenantID}), appPool, tenantID, fn)
	}
	transactions := postgres.NewTransactionRepo()

	consumer := &stream.Consumer{
		Deps: stream.WorkerDeps{
			TxRunner: txRunner, Transactions: transactions,
			MatchResults: postgres.NewMatchResultRepo(), MatchRules: postgres.NewMatchRuleRepo(),
			Registry: rules.DefaultRegistry(), Pool: pool,
		},
	}

	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup(consumerGroup),
		kgo.ConsumeTopics(streamTopic),
		kgo.DisableAutoCommit(),
	)
	if err != nil {
		log.Fatalf("matching-stream: new kafka client: %v", err)
	}
	defer client.Close()

	registerLagGauge(ctx, brokers)

	log.Printf("matching-stream: consuming %s as group %s", streamTopic, consumerGroup)
	runConsumeLoop(ctx, client, consumer, txRunner, transactions)
}

// runConsumeLoop dispatches each fetched record to one of numWorkers
// keyed goroutines (hash of the record key, which the producer sets to
// tenant_id/account_id per task 18's topic layout), so records for the
// SAME account are always processed in order by the SAME goroutine
// (plans/task/core/19 Implementation Notes: "do not process the same
// account-pair's events out of order across goroutines").
func runConsumeLoop(ctx context.Context, client *kgo.Client, consumer *stream.Consumer, txRunner stream.TxRunner, transactions domain.TransactionRepository) {
	workers := make([]chan *kgo.Record, numWorkers)
	for i := range workers {
		workers[i] = make(chan *kgo.Record, 100)
		go func(ch <-chan *kgo.Record) {
			for rec := range ch {
				processRecord(ctx, consumer, txRunner, transactions, rec)
			}
		}(workers[i])
	}

	for {
		if ctx.Err() != nil {
			return
		}
		fetches := client.PollFetches(ctx)
		if ctx.Err() != nil {
			return
		}
		fetches.EachRecord(func(rec *kgo.Record) {
			workers[workerIndex(rec.Key, numWorkers)] <- rec
		})
		if err := client.CommitUncommittedOffsets(ctx); err != nil && ctx.Err() == nil {
			slog.ErrorContext(ctx, "matching-stream: commit offsets failed", "error", err)
		}
	}
}

func workerIndex(key []byte, n int) int {
	h := fnv.New32a()
	_, _ = h.Write(key)
	return int(h.Sum32()) % n
}

func processRecord(ctx context.Context, consumer *stream.Consumer, txRunner stream.TxRunner, transactions domain.TransactionRepository, rec *kgo.Record) {
	var evt jenginev1.TransactionEvent
	if err := proto.Unmarshal(rec.Value, &evt); err != nil {
		slog.ErrorContext(ctx, "matching-stream: unmarshal TransactionEvent failed", "error", err)
		return
	}
	tenantID, err := uuid.Parse(evt.GetTenantId())
	if err != nil {
		slog.ErrorContext(ctx, "matching-stream: invalid tenant_id in event", "error", err)
		return
	}
	txnID, err := uuid.Parse(evt.GetTransactionId())
	if err != nil {
		slog.ErrorContext(ctx, "matching-stream: invalid transaction_id in event", "error", err)
		return
	}

	// The event is a notification that this transaction now exists in
	// the system of record (Postgres) - TransactionEvent's own fields
	// (plans/docs/06-streaming-architecture.md §7.2) don't carry every
	// field internal/matching/core.MatchableRecord needs (e.g. Side),
	// so the authoritative full row is loaded rather than reconstructed
	// from the event alone.
	var txn domain.Transaction
	err = txRunner(ctx, tenantID, func(ctx context.Context) error {
		var err error
		txn, err = transactions.GetByID(ctx, tenantID, txnID)
		return err
	})
	if err != nil {
		slog.ErrorContext(ctx, "matching-stream: load transaction failed", "tenant_id", tenantID, "transaction_id", txnID, "error", err)
		return
	}

	if err := consumer.Process(ctx, tenantID, txn); err != nil {
		slog.ErrorContext(ctx, "matching-stream: process failed", "tenant_id", tenantID, "transaction_id", txnID, "error", err)
	}
}

// registerLagGauge exports consumer-group lag as a Prometheus gauge
// (plans/task/core/19 Implementation Notes §7.4: "Export consumer-group
// lag as a Prometheus gauge; KEDA ScaledObject scales matching-stream
// replicas on that lag metric") - see deploy/helm/matching-stream for
// the ScaledObject reading it. Uses its own short-lived kadm.Client
// (admin operations, separate from the long-running consumer client
// above) since lag is a slowly-changing gauge computed via a metadata
// call, not something derived from the consume loop's own state.
func registerLagGauge(ctx context.Context, brokers []string) {
	adminClient, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		slog.ErrorContext(ctx, "matching-stream: lag gauge: new admin client failed", "error", err)
		return
	}
	admin := kadm.NewClient(adminClient)

	meter := otel.Meter("matching-stream")
	_, err = meter.Float64ObservableGauge("matching_stream_consumer_lag",
		metric.WithDescription("Total consumer-group lag (records) on "+streamTopic+" for group "+consumerGroup),
		metric.WithFloat64Callback(func(ctx context.Context, obs metric.Float64Observer) error {
			lags, lagErr := admin.Lag(ctx, consumerGroup)
			if lagErr != nil {
				return lagErr
			}
			obs.Observe(float64(lags[consumerGroup].Lag.Total()))
			return nil
		}),
	)
	if err != nil {
		slog.ErrorContext(ctx, "matching-stream: register lag gauge failed", "error", err)
	}
}
