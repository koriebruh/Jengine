package main

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/koriebruh/Jengine/internal/notify"
)

// runSeedDemo is the manual-verification helper plans/task/core/21's
// own Definition of Done asks for: "register a subscription pointing at
// a local mock receiver, trigger a cataloged event, observe signed
// delivery and a correctly-computed signature on the receiver side."
// Seeds a tenant + subscription (secret resolved via
// WEBHOOK_DEMO_SECRET, matching sftp.EnvSecretResolver's env-var
// convention) pointing at whatever URL WEBHOOK_DEMO_RECEIVER_URL names
// (default http://localhost:9999/webhook - point a mock receiver there
// before running this), then publishes one synthetic break.created
// event to case.events.default with the tenant_id/event_type headers
// the real outbox-router SMT would attach.
func runSeedDemo(ctx context.Context, brokers []string) error {
	superuserDSN := envOrDefault("DATABASE_URL", "postgres://jengine:jengine_dev@localhost:5432/jengine?sslmode=disable")
	pool, err := pgxpool.New(ctx, superuserDSN)
	if err != nil {
		return fmt.Errorf("connect to postgres: %w", err)
	}
	defer pool.Close()

	tenantID := uuid.New()
	if _, err := pool.Exec(ctx,
		`INSERT INTO tenants (id, name, isolation_tier, region, status) VALUES ($1, 'webhook-dispatcher demo', 'STANDARD', 'us-east', 'ACTIVE')`,
		tenantID,
	); err != nil {
		return fmt.Errorf("seed tenant: %w", err)
	}

	receiverURL := envOrDefault("WEBHOOK_DEMO_RECEIVER_URL", "http://localhost:9999/webhook")
	secretRef := "WEBHOOK_DEMO_SECRET"

	subID := uuid.New()
	if _, err := pool.Exec(ctx,
		`INSERT INTO webhook_subscriptions (id, tenant_id, url, secret_ref, event_types, status)
		 VALUES ($1, $2, $3, $4, $5, 'ACTIVE')`,
		subID, tenantID, receiverURL, secretRef, []string{notify.EventBreakCreated},
	); err != nil {
		return fmt.Errorf("seed subscription: %w", err)
	}

	breakID := uuid.New()
	payload := []byte(fmt.Sprintf(`{"break_id":"%s","break_type":"UNMATCHED","amount_at_risk":75000}`, breakID))

	client, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		return fmt.Errorf("new kafka client: %w", err)
	}
	defer client.Close()

	res := client.ProduceSync(ctx, &kgo.Record{
		Topic: "case.events.default",
		Key:   []byte(breakID.String()),
		Value: payload,
		Headers: []kgo.RecordHeader{
			{Key: "tenant_id", Value: []byte(tenantID.String())},
			{Key: "aggregate_type", Value: []byte("case")},
			{Key: "event_type", Value: []byte(notify.EventBreakCreated)},
		},
		Timestamp: time.Now(),
	})
	if err := res.FirstErr(); err != nil {
		return fmt.Errorf("publish event: %w", err)
	}

	fmt.Printf("seeded tenant_id=%s subscription_id=%s pointing at %s\n", tenantID, subID, receiverURL)
	fmt.Printf("set %s env var to the secret your mock receiver expects, then run `webhook-dispatcher` (default -demo=serve)\n", secretRef)
	fmt.Printf("published break.created event for break_id=%s to case.events.default\n", breakID)
	fmt.Printf("check webhook_deliveries: SELECT * FROM webhook_deliveries WHERE subscription_id = '%s';\n", subID)
	return nil
}
