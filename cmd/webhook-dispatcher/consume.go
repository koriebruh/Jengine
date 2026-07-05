package main

import (
	"context"
	"log/slog"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/riverqueue/river"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/koriebruh/Jengine/internal/domain"
	"github.com/koriebruh/Jengine/internal/notify"
)

// processRecord handles one case-event/matching-result record: resolves
// tenant_id/event_type from the record's headers (the same shape
// deploy/debezium/outbox-connector.json's outbox-event-router SMT
// attaches - tenant_id/aggregate_type/event_type, see task 18), matches
// it against ACTIVE subscriptions, and enqueues one River delivery job
// per match (plans/task/core/21 Implementation Notes: "fan out one
// WebhookDelivery row + one async HTTP POST per matching subscription -
// never block the consumer loop on a slow tenant endpoint").
func processRecord(ctx context.Context, rec *kgo.Record, riverClient *river.Client[pgx.Tx], subs domain.WebhookSubscriptionRepository, deliveries domain.WebhookDeliveryRepository, txRunner TxRunner, hub *Hub) {
	var tenantIDStr, eventType string
	for _, h := range rec.Headers {
		switch h.Key {
		case "tenant_id":
			tenantIDStr = string(h.Value)
		case "event_type":
			eventType = string(h.Value)
		}
	}
	if tenantIDStr == "" || eventType == "" {
		slog.WarnContext(ctx, "webhook-dispatcher: record missing tenant_id/event_type headers, skipping", "topic", rec.Topic)
		return
	}
	if !notify.IsCataloged(eventType) {
		// Not every event on these topics is necessarily webhook-
		// deliverable (e.g. internal bookkeeping events sharing the
		// same topic) - silently skipping an uncataloged type is
		// correct, not a bug to fail loudly over.
		return
	}
	tenantID, err := uuid.Parse(tenantIDStr)
	if err != nil {
		slog.ErrorContext(ctx, "webhook-dispatcher: invalid tenant_id header", "value", tenantIDStr, "error", err)
		return
	}

	payload := rec.Value // outbox-router SMT emits the raw payload as the record value (base64-decoded transport handled at the topic/converter level, see task 18/19's own docs on this)

	// SSE fan-out is independent of webhook subscription matching (a
	// tenant's connected browser sessions receive every cataloged event
	// regardless of whether any webhook subscription exists) - see this
	// file's sibling sse.go's own doc comment on the ownership split.
	hub.Broadcast(tenantID, sseEvent{EventType: eventType, Payload: payload})

	var candidates []domain.WebhookSubscription
	err = txRunner(ctx, tenantID, func(ctx context.Context) error {
		var err error
		candidates, err = subs.ListActiveByEventType(ctx, tenantID, eventType)
		return err
	})
	if err != nil {
		slog.ErrorContext(ctx, "webhook-dispatcher: list active subscriptions failed", "tenant_id", tenantID, "event_type", eventType, "error", err)
		return
	}
	if len(candidates) == 0 {
		return
	}

	matched := notify.MatchingSubscriptions(candidates, payload)
	for _, sub := range matched {
		var created domain.WebhookDelivery
		err := txRunner(ctx, tenantID, func(ctx context.Context) error {
			var err error
			created, err = deliveries.Create(ctx, tenantID, domain.WebhookDelivery{
				SubscriptionID: sub.ID, EventID: rec.Topic + "-" + string(rec.Key), EventType: eventType, Payload: payload,
			})
			return err
		})
		if err != nil {
			slog.ErrorContext(ctx, "webhook-dispatcher: create delivery row failed", "subscription_id", sub.ID, "error", err)
			continue
		}

		if _, err := riverClient.Insert(ctx, DeliveryJobArgs{
			DeliveryID: created.ID, TenantID: tenantID, SubscriptionID: sub.ID,
		}, &river.InsertOpts{MaxAttempts: notify.DefaultMaxAttempts}); err != nil {
			slog.ErrorContext(ctx, "webhook-dispatcher: enqueue delivery job failed", "delivery_id", created.ID, "error", err)
		}
	}
}
