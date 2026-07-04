package ingestion

import (
	"context"
	"time"

	"github.com/google/uuid"
)

// QuarantineEntry mirrors the quarantine_entries table
// (migrations/0003_quarantine_entries.up.sql) - added in this task since
// plans/docs/02-data-ingestion.md §3.3 requires a durable quarantine
// queue that task 03's original entity list didn't include.
type QuarantineEntry struct {
	ID          uuid.UUID
	TenantID    uuid.UUID
	ConnectorID *uuid.UUID
	Stage       string
	Reason      string
	RawPayload  []byte
	OccurredAt  time.Time
}

// QuarantineSink is where invalid/failed records land, per
// plans/docs/02-data-ingestion.md §3.3: "failures land in quarantine
// queue... never silently drop financial data." Not log-only - a
// queryable persisted record (plans/task/core/06 Common Pitfalls). The
// concrete Postgres-backed implementation lives in
// internal/storage/postgres (quarantine_repo.go), alongside every other
// repository in this codebase, following the same requireTx/ambient-
// transaction pattern (plans/task/core/05).
type QuarantineSink interface {
	Quarantine(ctx context.Context, tenantID, connectorID uuid.UUID, stage, reason string, payload []byte) error
	List(ctx context.Context, tenantID uuid.UUID, connectorID uuid.UUID) ([]QuarantineEntry, error)
}
