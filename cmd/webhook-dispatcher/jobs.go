package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/rivertype"

	"github.com/koriebruh/Jengine/internal/domain"
	"github.com/koriebruh/Jengine/internal/notify"
)

// maxResponseSnippet bounds how much of a receiver's response body is
// stored - plans/task/core/21's own data model comment: "truncated
// (e.g. first 2KB) - never store unbounded response bodies."
const maxResponseSnippet = 2 << 10 // 2KB

// SecretResolver resolves a Vault path reference to its secret value -
// same shape/rationale as every other connector/dispatcher package's
// own local copy in this codebase (see connector/sftp.SecretResolver's
// doc comment for the precedent).
type SecretResolver interface {
	Resolve(ctx context.Context, vaultPathRef string) (string, error)
}

// DeliveryJobArgs is one webhook delivery attempt - one job per
// (delivery row, HTTP attempt); River's own retry mechanism re-runs
// Work() on failure per DeliveryWorker.NextRetry, it doesn't requeue a
// new job each time.
type DeliveryJobArgs struct {
	DeliveryID     uuid.UUID
	TenantID       uuid.UUID
	SubscriptionID uuid.UUID
}

func (DeliveryJobArgs) Kind() string { return "webhook_delivery" }

// DeliveryDeps are DeliveryWorker's dependencies.
type DeliveryDeps struct {
	TxRunner      TxRunner
	Deliveries    domain.WebhookDeliveryRepository
	Subscriptions domain.WebhookSubscriptionRepository
	Secrets       SecretResolver
	HTTPClient    *http.Client
}

// TxRunner wraps fn in a transaction scoped to tenantID - same shape as
// every other package's own local copy in this codebase.
type TxRunner func(ctx context.Context, tenantID uuid.UUID, fn func(ctx context.Context) error) error

// DeliveryWorker implements river.Worker[DeliveryJobArgs] - one HTTP
// delivery attempt per Work() call (plans/task/core/21 Implementation
// Notes: "never block the consumer loop on a slow tenant endpoint; use
// a bounded worker pool per delivery attempt" - River's own worker pool
// IS that bounded pool).
type DeliveryWorker struct {
	river.WorkerDefaults[DeliveryJobArgs]
	Deps DeliveryDeps
}

// NextRetry reuses notify's documented exponential-backoff-with-jitter
// schedule (plans/task/core/21: "Reuse River... for retry scheduling
// rather than hand-rolling a poller") instead of River's own default
// policy, since the design specifies exact intervals (1m/5m/30m/2h/12h).
func (w *DeliveryWorker) NextRetry(job *river.Job[DeliveryJobArgs]) time.Time {
	return time.Now().Add(notify.NextAttemptDelay(job.Attempt))
}

func (w *DeliveryWorker) Work(ctx context.Context, job *river.Job[DeliveryJobArgs]) error {
	args := job.Args

	var delivery domain.WebhookDelivery
	var sub domain.WebhookSubscription
	err := w.Deps.TxRunner(ctx, args.TenantID, func(ctx context.Context) error {
		var err error
		delivery, err = w.Deps.Deliveries.GetByID(ctx, args.TenantID, args.DeliveryID)
		if err != nil {
			return fmt.Errorf("load delivery: %w", err)
		}
		sub, err = w.Deps.Subscriptions.GetByID(ctx, args.TenantID, args.SubscriptionID)
		return err
	})
	if err != nil {
		return fmt.Errorf("webhook-dispatcher: load delivery/subscription: %w", err)
	}

	secret, err := w.Deps.Secrets.Resolve(ctx, sub.SecretRef)
	if err != nil {
		return fmt.Errorf("webhook-dispatcher: resolve secret: %w", err)
	}

	now := time.Now()
	signature := notify.Sign(secret, now, delivery.Payload)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, sub.URL, bytes.NewReader(delivery.Payload))
	if err != nil {
		return fmt.Errorf("webhook-dispatcher: build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Jengine-Signature", signature)
	req.Header.Set("X-Jengine-Event-Type", delivery.EventType)

	resp, httpErr := w.Deps.HTTPClient.Do(req)

	attemptCount := delivery.AttemptCount + 1
	var status domain.WebhookDeliveryStatus
	var responseStatus *int
	var responseSnippet *string
	var nextAttemptAt *time.Time
	var workErr error

	switch {
	case httpErr != nil:
		status = domain.WebhookDeliveryStatusFailed
		s := httpErr.Error()
		if len(s) > maxResponseSnippet {
			s = s[:maxResponseSnippet]
		}
		responseSnippet = &s
		workErr = fmt.Errorf("webhook-dispatcher: delivery request failed: %w", httpErr)
	case resp.StatusCode >= 200 && resp.StatusCode < 300:
		status = domain.WebhookDeliveryStatusDelivered
		responseStatus = &resp.StatusCode
	default:
		status = domain.WebhookDeliveryStatusFailed
		responseStatus = &resp.StatusCode
		body, _ := io.ReadAll(io.LimitReader(resp.Body, maxResponseSnippet))
		snippet := string(body)
		responseSnippet = &snippet
		workErr = fmt.Errorf("webhook-dispatcher: delivery returned non-2xx status %d", resp.StatusCode)
	}
	if resp != nil {
		_ = resp.Body.Close()
	}

	if status == domain.WebhookDeliveryStatusFailed && job.Attempt < job.MaxAttempts {
		next := now.Add(notify.NextAttemptDelay(attemptCount))
		nextAttemptAt = &next
	}

	if err := w.Deps.TxRunner(ctx, args.TenantID, func(ctx context.Context) error {
		return w.Deps.Deliveries.UpdateAfterAttempt(ctx, args.TenantID, args.DeliveryID, attemptCount, status, now, nextAttemptAt, responseStatus, responseSnippet)
	}); err != nil {
		return fmt.Errorf("webhook-dispatcher: record attempt outcome: %w", err)
	}

	return workErr
}

var _ river.Worker[DeliveryJobArgs] = (*DeliveryWorker)(nil)

// DeadLetterHandler implements river.ErrorHandler - when a delivery
// job's LAST attempt fails (job.Attempt >= job.MaxAttempts, so River
// itself is about to discard it, not schedule another retry), mark the
// WebhookDelivery row DEAD_LETTERED. Ordinary (non-final) failures are
// already recorded as FAILED by DeliveryWorker.Work itself with
// next_attempt_at set - this handler only fires the terminal
// transition.
type DeadLetterHandler struct {
	TxRunner   TxRunner
	Deliveries domain.WebhookDeliveryRepository
}

func (h *DeadLetterHandler) HandleError(ctx context.Context, job *rivertype.JobRow, err error) *river.ErrorHandlerResult {
	if job.Attempt < job.MaxAttempts {
		return nil
	}
	var args DeliveryJobArgs
	if unmarshalErr := json.Unmarshal(job.EncodedArgs, &args); unmarshalErr != nil {
		return nil
	}
	_ = h.TxRunner(ctx, args.TenantID, func(ctx context.Context) error {
		return h.Deliveries.UpdateAfterAttempt(ctx, args.TenantID, args.DeliveryID, job.Attempt, domain.WebhookDeliveryStatusDeadLettered, time.Now(), nil, nil, nil)
	})
	return nil
}

func (h *DeadLetterHandler) HandlePanic(ctx context.Context, job *rivertype.JobRow, panicVal any, trace string) *river.ErrorHandlerResult {
	return nil
}

var _ river.ErrorHandler = (*DeadLetterHandler)(nil)
