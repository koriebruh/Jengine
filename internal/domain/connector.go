package domain

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

// Connector mirrors the connectors table. Config's secrets are Vault path
// references, never inline values - plans/docs/02-data-ingestion.md §3.1;
// enforcing that is the connector framework's job (plans/task/core/06),
// not this struct's.
type Connector struct {
	ID          uuid.UUID
	TenantID    uuid.UUID
	Type        string
	Config      json.RawMessage
	Schedule    *string
	Status      string
	LastRunAt   *time.Time
	CursorState json.RawMessage
	CreatedAt   time.Time
	UpdatedAt   time.Time
}
