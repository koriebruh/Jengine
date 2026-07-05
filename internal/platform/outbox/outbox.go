// Package outbox implements the general transactional-outbox write path
// (plans/task/core/18, plans/docs/06-streaming-architecture.md §7.3):
// any code path that changes domain state and needs to emit an event
// inserts into outbox_event in the SAME database transaction as the
// state change, never a separate produce-to-Kafka call after commit -
// that separate-call pattern is exactly the dual-write bug this
// package exists to prevent.
//
// Consumption is via Debezium's outbox-event-router single-message-
// transform reading outbox_event's CDC stream (plans/task/core/18
// Implementation Notes), not a Go poller - this package only writes,
// it never reads/relays. Contrast with internal/ingestion's
// OutboxWriter/OutboxReader/OutboxRelay (plans/task/core/06/09), a
// separate, simpler poll-based mechanism for the ingestion pipeline's
// own narrower need; QA_REPORT.md documents the two mechanisms'
// overlapping intent as an open architectural question, not something
// this package silently resolves.
package outbox

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// Event is one row to insert into outbox_event. Payload is an already-
// serialized protobuf message - this package doesn't know or care about
// the concrete message type, keeping it decoupled from any specific
// event schema.
type Event struct {
	AggregateType string // 'transaction' | 'match_result' | 'break' | 'webhook' | ...
	AggregateID   uuid.UUID
	EventType     string
	Topic         string
	Payload       []byte
}

// Insert writes evt into outbox_event using tx - the caller's own
// already-open transaction, so this insert commits atomically with
// whatever domain-state change it's paired with. Deliberately takes an
// explicit pgx.Tx parameter (not an ambient-context lookup like this
// codebase's domain repositories use) so this package has no dependency
// on internal/storage/postgres or any other layer - it's a standalone,
// minimal write primitive any package can call given a transaction it
// already has.
func Insert(ctx context.Context, tx pgx.Tx, tenantID uuid.UUID, evt Event) error {
	_, err := tx.Exec(ctx,
		`INSERT INTO outbox_event (tenant_id, aggregate_type, aggregate_id, event_type, topic, payload)
		 VALUES ($1, $2, $3, $4, $5, $6)`,
		tenantID, evt.AggregateType, evt.AggregateID, evt.EventType, evt.Topic, evt.Payload,
	)
	if err != nil {
		return fmt.Errorf("outbox: insert: %w", err)
	}
	return nil
}
