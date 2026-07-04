package domain

import (
	"time"

	"github.com/google/uuid"
)

// IngestionDedupEntry mirrors the ingestion_dedup table
// (plans/task/core/03) - the authoritative dedup guard, backed by a
// UNIQUE (tenant_id, idempotency_key) constraint.
type IngestionDedupEntry struct {
	ID                uuid.UUID
	TenantID          uuid.UUID
	IdempotencyKey    string
	SourceConnectorID *uuid.UUID
	IngestionBatchID  string
	CreatedAt         time.Time
}
