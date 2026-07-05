package domain

import (
	"time"

	"github.com/google/uuid"
)

type WebhookSubscriptionStatus string

const (
	WebhookSubscriptionStatusActive   WebhookSubscriptionStatus = "ACTIVE"
	WebhookSubscriptionStatusPaused   WebhookSubscriptionStatus = "PAUSED"
	WebhookSubscriptionStatusDisabled WebhookSubscriptionStatus = "DISABLED"
)

// WebhookSubscription mirrors webhook_subscriptions (plans/task/core/21).
type WebhookSubscription struct {
	ID         uuid.UUID
	TenantID   uuid.UUID
	URL        string
	SecretRef  string // Vault path reference, never an inline secret
	EventTypes []string
	FilterExpr *string
	Status     WebhookSubscriptionStatus
	CreatedAt  time.Time
	UpdatedAt  time.Time
}

type WebhookDeliveryStatus string

const (
	WebhookDeliveryStatusPending      WebhookDeliveryStatus = "PENDING"
	WebhookDeliveryStatusDelivered    WebhookDeliveryStatus = "DELIVERED"
	WebhookDeliveryStatusFailed       WebhookDeliveryStatus = "FAILED"
	WebhookDeliveryStatusDeadLettered WebhookDeliveryStatus = "DEAD_LETTERED"
)

// WebhookDelivery mirrors webhook_deliveries - one row per (event,
// subscription) delivery attempt history.
type WebhookDelivery struct {
	ID                  uuid.UUID
	TenantID            uuid.UUID
	SubscriptionID      uuid.UUID
	EventID             string
	EventType           string
	Payload             []byte
	AttemptCount        int
	Status              WebhookDeliveryStatus
	LastAttemptAt       *time.Time
	NextAttemptAt       *time.Time
	ResponseStatus      *int
	ResponseBodySnippet *string
	CreatedAt           time.Time
}
