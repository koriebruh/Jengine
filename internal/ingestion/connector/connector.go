package connector

import (
	"context"
	"encoding/json"
	"time"

	"github.com/google/uuid"

	"github.com/koriebruh/Jengine/internal/domain"
)

// SourceConnector is the extensibility contract for data ingestion,
// implemented verbatim from plans/docs/02-data-ingestion.md §3.1 - do not
// add methods or change Fetch's channel-based return type "for
// convenience" (plans/task/core/06 Common Pitfalls). A WASM-backed
// connector (plans/task/core/25, V1) must be able to satisfy this same
// interface later without a breaking change.
type SourceConnector interface {
	Fetch(ctx context.Context, cfg ConnectorConfig) (<-chan RawRecord, error)
	Validate(cfg ConnectorConfig) error
	SupportsStreaming() bool
	Checkpoint() (Cursor, error)
}

// RawRecord is one unparsed record as received from a connector, before
// any parsing/mapping/normalization. SourceFormat matches
// plans/task/core/08's mapping DSL source_format key (e.g. "MT940",
// "CSV").
type RawRecord struct {
	TenantID     uuid.UUID
	ConnectorID  uuid.UUID
	SourceFormat string
	Payload      []byte
	ReceivedAt   time.Time
	BatchID      uuid.UUID // groups records from one fetch/file/poll cycle
	SourceMode   domain.SourceMode
}

// ConnectorConfig is the tenant-scoped configuration a SourceConnector is
// constructed/run with. Settings' secrets are Vault path references,
// never inline values (plans/docs/02-data-ingestion.md §3.1,
// plans/docs/16-development-workflow.md §16.3).
type ConnectorConfig struct {
	ConnectorID uuid.UUID
	TenantID    uuid.UUID
	Type        string
	Settings    json.RawMessage
	Schedule    string // cron expression; empty for pure-streaming connectors
}

// Cursor is a connector's opaque watermark/offset, persisted between
// runs.
type Cursor struct {
	ConnectorID uuid.UUID
	State       json.RawMessage
	UpdatedAt   time.Time
}
