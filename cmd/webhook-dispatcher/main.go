// Command webhook-dispatcher runs plans/task/core/21's outbound webhook
// delivery: consumes case-event topics, resolves matching ACTIVE
// subscriptions, creates a WebhookDelivery row per match, and delivers
// via a bounded River-backed worker pool with exponential-backoff
// retry to DEAD_LETTERED. Default mode ("serve") is the long-running
// consumer+worker. Pass -demo=seed to register a test subscription and
// publish one synthetic event for manual verification.
//
// Consumes case.events.default and matching.results.default (the
// topics tasks 19/20's Activities/reconciler ACTUALLY publish to, via
// task 18's outbox pattern) rather than the "webhook.outbox" topic this
// task's own text names - a deliberate, documented deviation; see
// QA_REPORT.md.
package main

import (
	"context"
	"flag"
	"log"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/koriebruh/Jengine/internal/domain"
	"github.com/koriebruh/Jengine/internal/ingestion/connector/sftp"
	"github.com/koriebruh/Jengine/internal/matching/batch"
	"github.com/koriebruh/Jengine/internal/storage/postgres"
	"github.com/koriebruh/Jengine/internal/tenancy"
)

var caseEventTopics = []string{"case.events.default", "matching.results.default"}

const dispatcherTaskQueue = "webhook-dispatcher"

func envOrDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func newTxRunner(pool *pgxpool.Pool) TxRunner {
	return func(ctx context.Context, tenantID uuid.UUID, fn func(ctx context.Context) error) error {
		return postgres.WithTx(tenancy.WithTenant(ctx, tenancy.TenantContext{TenantID: tenantID}), pool, tenantID, fn)
	}
}

func main() {
	demo := flag.String("demo", "serve", "\"serve\" (default; long-running consumer+worker) or \"seed\" (manual verification helper)")
	flag.Parse()

	ctx := context.Background()
	brokers := []string{envOrDefault("REDPANDA_BROKER", "localhost:9092")}

	if *demo == "seed" {
		if err := runSeedDemo(ctx, brokers); err != nil {
			log.Fatalf("webhook-dispatcher: seed demo: %v", err)
		}
		return
	}

	superuserDSN := envOrDefault("DATABASE_URL", "postgres://jengine:jengine_dev@localhost:5432/jengine?sslmode=disable")
	appDSN := envOrDefault("APP_DATABASE_URL", "postgres://jengine_app:jengine_app_dev@localhost:5432/jengine?sslmode=disable")

	superuserPool, err := pgxpool.New(ctx, superuserDSN)
	if err != nil {
		log.Fatalf("webhook-dispatcher: connect as superuser: %v", err)
	}
	defer superuserPool.Close()

	appPool, err := pgxpool.New(ctx, appDSN)
	if err != nil {
		log.Fatalf("webhook-dispatcher: connect as jengine_app: %v", err)
	}
	defer appPool.Close()

	if err := ensureRiverSchema(ctx, superuserPool); err != nil {
		log.Fatalf("webhook-dispatcher: ensure river schema: %v", err)
	}

	txRunner := newTxRunner(appPool)
	deps := DeliveryDeps{
		TxRunner: txRunner, Deliveries: postgres.NewWebhookDeliveryRepo(),
		Subscriptions: postgres.NewWebhookSubscriptionRepo(), Secrets: sftp.EnvSecretResolver{},
		HTTPClient: &http.Client{Timeout: 10 * time.Second},
	}
	worker := &DeliveryWorker{Deps: deps}

	workers := river.NewWorkers()
	if err := river.AddWorkerSafely(workers, worker); err != nil {
		log.Fatalf("webhook-dispatcher: register delivery worker: %v", err)
	}
	riverClient, err := river.NewClient(riverpgxv5.New(superuserPool), &river.Config{
		Queues:       map[string]river.QueueConfig{river.QueueDefault: {MaxWorkers: 20}},
		Workers:      workers,
		ErrorHandler: &DeadLetterHandler{TxRunner: txRunner, Deliveries: deps.Deliveries},
	})
	if err != nil {
		log.Fatalf("webhook-dispatcher: new river client: %v", err)
	}
	if err := riverClient.Start(ctx); err != nil {
		log.Fatalf("webhook-dispatcher: start river client: %v", err)
	}
	defer func() { _ = riverClient.Stop(ctx) }()

	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup(dispatcherTaskQueue),
		kgo.ConsumeTopics(caseEventTopics...),
		kgo.DisableAutoCommit(),
	)
	if err != nil {
		log.Fatalf("webhook-dispatcher: new kafka client: %v", err)
	}
	defer client.Close()

	// SSE gateway (plans/task/core/21's own subsection) - a second,
	// lightweight consumer of the same Kafka records this process
	// already reads for webhook delivery, served as an additional
	// entrypoint inside this same binary rather than a sibling
	// cmd/realtime-gateway (this task's own documented either-is-fine
	// choice).
	hub := NewHub()
	streamTokenSecret := envOrDefault("STREAM_TOKEN_SECRET", "dev-only-insecure-stream-token-secret")
	sseMux := http.NewServeMux()
	sseMux.HandleFunc("GET /v1/tenants/{tenant_id}/events/stream", streamHandler(hub, streamTokenSecret))
	sseAddr := envOrDefault("SSE_ADDR", ":8082")
	go func() {
		log.Printf("webhook-dispatcher: SSE gateway listening on %s", sseAddr)
		if err := http.ListenAndServe(sseAddr, sseMux); err != nil { //nolint:gosec // dev-only plain HTTP, matches every other cmd/* binary's own listener setup in this repo
			log.Fatalf("webhook-dispatcher: SSE gateway: %v", err)
		}
	}()

	log.Printf("webhook-dispatcher: consuming %v as group %s", caseEventTopics, dispatcherTaskQueue)
	runConsumeLoop(ctx, client, riverClient, postgres.NewWebhookSubscriptionRepo(), postgres.NewWebhookDeliveryRepo(), txRunner, hub)
}

func ensureRiverSchema(ctx context.Context, pool *pgxpool.Pool) error {
	// River's own schema is global (not matching-specific) - reuse
	// plans/task/core/12's existing helper rather than duplicating the
	// rivermigrate wiring here.
	return batch.EnsureRiverSchema(ctx, pool)
}

func runConsumeLoop(ctx context.Context, client *kgo.Client, riverClient *river.Client[pgx.Tx], subs domain.WebhookSubscriptionRepository, deliveries domain.WebhookDeliveryRepository, txRunner TxRunner, hub *Hub) {
	for {
		if ctx.Err() != nil {
			return
		}
		fetches := client.PollFetches(ctx)
		if ctx.Err() != nil {
			return
		}
		fetches.EachRecord(func(rec *kgo.Record) {
			processRecord(ctx, rec, riverClient, subs, deliveries, txRunner, hub)
		})
		if err := client.CommitUncommittedOffsets(ctx); err != nil && ctx.Err() == nil {
			slog.ErrorContext(ctx, "webhook-dispatcher: commit offsets failed", "error", err)
		}
	}
}
