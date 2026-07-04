package ingestion

import (
	"context"
	"time"

	"github.com/google/uuid"
)

// OutboxEvent mirrors the ingestion_outbox table
// (migrations/0004_ingestion_outbox.up.sql).
type OutboxEvent struct {
	ID        uuid.UUID
	TenantID  uuid.UUID
	Topic     string
	Key       string
	Payload   []byte
	CreatedAt time.Time
	SentAt    *time.Time
}

// OutboxWriter is the tenant-scoped write half of the transactional
// outbox pattern (plans/docs/06-streaming-architecture.md §7.3) - called
// from within the same DB transaction as the domain row it accompanies
// (see internal/storage/postgres's PersistEmitStage), never as a
// separate dual write.
type OutboxWriter interface {
	Write(ctx context.Context, tenantID uuid.UUID, topic, key string, payload []byte) error
}

// OutboxReader is the cross-tenant read/relay half - deliberately NOT
// tenant-scoped (no tenantID parameter), since a relay sweeping unsent
// events across all tenants is genuinely cross-tenant infrastructure, the
// same class of exception as internal/tenancy's RegistryRepo. See
// internal/storage/postgres/outbox_repo.go for why this must run against
// an RLS-bypassing (superuser/migration) connection, not the per-tenant
// jengine_app pool.
type OutboxReader interface {
	ListUnsent(ctx context.Context, limit int) ([]OutboxEvent, error)
	MarkSent(ctx context.Context, id uuid.UUID) error
}
