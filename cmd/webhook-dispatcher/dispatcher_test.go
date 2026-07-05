package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/rivertype"

	"github.com/koriebruh/Jengine/internal/domain"
	"github.com/koriebruh/Jengine/internal/notify"
	"github.com/koriebruh/Jengine/internal/storage/postgres"
	"github.com/koriebruh/Jengine/internal/tenancy"
	"github.com/koriebruh/Jengine/internal/testutil"
)

var errFakeDeliveryFailure = errors.New("simulated final delivery failure")

type staticSecrets struct{ secret string }

func (s staticSecrets) Resolve(ctx context.Context, ref string) (string, error) { return s.secret, nil }

func seedSubAndDelivery(t *testing.T, ctx context.Context, txRunner TxRunner, tenantID uuid.UUID, url string) (domain.WebhookSubscription, domain.WebhookDelivery) {
	t.Helper()
	subs := postgres.NewWebhookSubscriptionRepo()
	deliveries := postgres.NewWebhookDeliveryRepo()

	var sub domain.WebhookSubscription
	var delivery domain.WebhookDelivery
	err := txRunner(ctx, tenantID, func(ctx context.Context) error {
		var err error
		sub, err = subs.Create(ctx, tenantID, domain.WebhookSubscription{
			URL: url, SecretRef: "test-secret-ref", EventTypes: []string{notify.EventBreakCreated},
			Status: domain.WebhookSubscriptionStatusActive,
		})
		if err != nil {
			return err
		}
		delivery, err = deliveries.Create(ctx, tenantID, domain.WebhookDelivery{
			SubscriptionID: sub.ID, EventID: "evt-1", EventType: notify.EventBreakCreated,
			Payload: []byte(`{"break_id":"test"}`),
		})
		return err
	})
	if err != nil {
		t.Fatalf("seed subscription/delivery failed: %v", err)
	}
	return sub, delivery
}

func TestDeliveryWorker_Work_SuccessfulDelivery(t *testing.T) {
	if testing.Short() {
		t.Skip("requires Docker - skipped under -short; run make test-integration")
	}

	var receivedSig string
	mock := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedSig = r.Header.Get("X-Jengine-Signature")
		w.WriteHeader(http.StatusOK)
	}))
	defer mock.Close()

	db := testutil.StartPostgres(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	tenantID := uuid.New()
	if _, err := db.Pool.Exec(ctx,
		`INSERT INTO tenants (id, name, isolation_tier, region, status) VALUES ($1, 'Acme', 'STANDARD', 'us-east', 'ACTIVE')`,
		tenantID,
	); err != nil {
		t.Fatalf("seed tenant failed: %v", err)
	}

	appPool := testutil.AppRolePool(t, ctx, db.DSN)
	defer appPool.Close()
	txRunner := func(ctx context.Context, tid uuid.UUID, fn func(ctx context.Context) error) error {
		return postgres.WithTx(tenancy.WithTenant(ctx, tenancy.TenantContext{TenantID: tid}), appPool, tid, fn)
	}

	_, delivery := seedSubAndDelivery(t, ctx, txRunner, tenantID, mock.URL)

	worker := &DeliveryWorker{Deps: DeliveryDeps{
		TxRunner: txRunner, Deliveries: postgres.NewWebhookDeliveryRepo(), Subscriptions: postgres.NewWebhookSubscriptionRepo(),
		Secrets: staticSecrets{secret: "test-secret"}, HTTPClient: http.DefaultClient,
	}}

	job := &river.Job[DeliveryJobArgs]{
		JobRow: &rivertype.JobRow{Attempt: 1, MaxAttempts: notify.DefaultMaxAttempts},
		Args:   DeliveryJobArgs{DeliveryID: delivery.ID, TenantID: tenantID, SubscriptionID: delivery.SubscriptionID},
	}
	if err := worker.Work(ctx, job); err != nil {
		t.Fatalf("Work() failed: %v", err)
	}
	if receivedSig == "" {
		t.Error("expected mock receiver to see a signature header")
	}

	var status string
	if err := db.Pool.QueryRow(ctx, `SELECT status FROM webhook_deliveries WHERE id = $1`, delivery.ID).Scan(&status); err != nil {
		t.Fatalf("query delivery status failed: %v", err)
	}
	if status != string(domain.WebhookDeliveryStatusDelivered) {
		t.Errorf("expected DELIVERED, got %s", status)
	}
}

func TestDeliveryWorker_Work_FailureSetsNextAttempt(t *testing.T) {
	if testing.Short() {
		t.Skip("requires Docker - skipped under -short; run make test-integration")
	}

	mock := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer mock.Close()

	db := testutil.StartPostgres(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	tenantID := uuid.New()
	if _, err := db.Pool.Exec(ctx,
		`INSERT INTO tenants (id, name, isolation_tier, region, status) VALUES ($1, 'Acme', 'STANDARD', 'us-east', 'ACTIVE')`,
		tenantID,
	); err != nil {
		t.Fatalf("seed tenant failed: %v", err)
	}

	appPool := testutil.AppRolePool(t, ctx, db.DSN)
	defer appPool.Close()
	txRunner := func(ctx context.Context, tid uuid.UUID, fn func(ctx context.Context) error) error {
		return postgres.WithTx(tenancy.WithTenant(ctx, tenancy.TenantContext{TenantID: tid}), appPool, tid, fn)
	}

	_, delivery := seedSubAndDelivery(t, ctx, txRunner, tenantID, mock.URL)

	worker := &DeliveryWorker{Deps: DeliveryDeps{
		TxRunner: txRunner, Deliveries: postgres.NewWebhookDeliveryRepo(), Subscriptions: postgres.NewWebhookSubscriptionRepo(),
		Secrets: staticSecrets{secret: "test-secret"}, HTTPClient: http.DefaultClient,
	}}

	job := &river.Job[DeliveryJobArgs]{
		JobRow: &rivertype.JobRow{Attempt: 1, MaxAttempts: notify.DefaultMaxAttempts},
		Args:   DeliveryJobArgs{DeliveryID: delivery.ID, TenantID: tenantID, SubscriptionID: delivery.SubscriptionID},
	}
	if err := worker.Work(ctx, job); err == nil {
		t.Fatal("expected Work() to return an error for a non-2xx response")
	}

	var status string
	var nextAttemptAt *time.Time
	var responseStatus *int
	if err := db.Pool.QueryRow(ctx, `SELECT status, next_attempt_at, response_status FROM webhook_deliveries WHERE id = $1`, delivery.ID).
		Scan(&status, &nextAttemptAt, &responseStatus); err != nil {
		t.Fatalf("query delivery failed: %v", err)
	}
	if status != string(domain.WebhookDeliveryStatusFailed) {
		t.Errorf("expected FAILED, got %s", status)
	}
	if nextAttemptAt == nil {
		t.Error("expected next_attempt_at to be set for a retryable failure")
	}
	if responseStatus == nil || *responseStatus != http.StatusInternalServerError {
		t.Errorf("expected response_status 500, got %v", responseStatus)
	}
}

func TestDeadLetterHandler_HandleError_MarksDeadLetteredOnFinalAttempt(t *testing.T) {
	if testing.Short() {
		t.Skip("requires Docker - skipped under -short; run make test-integration")
	}

	db := testutil.StartPostgres(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	tenantID := uuid.New()
	if _, err := db.Pool.Exec(ctx,
		`INSERT INTO tenants (id, name, isolation_tier, region, status) VALUES ($1, 'Acme', 'STANDARD', 'us-east', 'ACTIVE')`,
		tenantID,
	); err != nil {
		t.Fatalf("seed tenant failed: %v", err)
	}

	appPool := testutil.AppRolePool(t, ctx, db.DSN)
	defer appPool.Close()
	txRunner := func(ctx context.Context, tid uuid.UUID, fn func(ctx context.Context) error) error {
		return postgres.WithTx(tenancy.WithTenant(ctx, tenancy.TenantContext{TenantID: tid}), appPool, tid, fn)
	}

	_, delivery := seedSubAndDelivery(t, ctx, txRunner, tenantID, "http://example.invalid/webhook")

	deliveries := postgres.NewWebhookDeliveryRepo()
	handler := &DeadLetterHandler{TxRunner: txRunner, Deliveries: deliveries}

	argsJSON, err := json.Marshal(DeliveryJobArgs{DeliveryID: delivery.ID, TenantID: tenantID, SubscriptionID: delivery.SubscriptionID})
	if err != nil {
		t.Fatalf("marshal job args failed: %v", err)
	}

	// Simulate River about to discard this job: Attempt has reached
	// MaxAttempts, so this is the FINAL failure, not an ordinary retry.
	jobRow := &rivertype.JobRow{Attempt: notify.DefaultMaxAttempts, MaxAttempts: notify.DefaultMaxAttempts, EncodedArgs: argsJSON}
	handler.HandleError(ctx, jobRow, errFakeDeliveryFailure)

	var status string
	if err := db.Pool.QueryRow(ctx, `SELECT status FROM webhook_deliveries WHERE id = $1`, delivery.ID).Scan(&status); err != nil {
		t.Fatalf("query delivery status failed: %v", err)
	}
	if status != string(domain.WebhookDeliveryStatusDeadLettered) {
		t.Errorf("expected DEAD_LETTERED after final attempt, got %s", status)
	}

	// --- Redrive ---
	if err := txRunner(ctx, tenantID, func(ctx context.Context) error {
		return deliveries.Redrive(ctx, tenantID, delivery.ID)
	}); err != nil {
		t.Fatalf("Redrive failed: %v", err)
	}

	var afterRedriveStatus string
	var attemptCount int
	if err := db.Pool.QueryRow(ctx, `SELECT status, attempt_count FROM webhook_deliveries WHERE id = $1`, delivery.ID).
		Scan(&afterRedriveStatus, &attemptCount); err != nil {
		t.Fatalf("query delivery after redrive failed: %v", err)
	}
	if afterRedriveStatus != string(domain.WebhookDeliveryStatusPending) {
		t.Errorf("expected PENDING after redrive, got %s", afterRedriveStatus)
	}
	if attemptCount != 0 {
		t.Errorf("expected attempt_count reset to 0 after redrive, got %d", attemptCount)
	}
}
