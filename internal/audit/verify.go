package audit

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Store is the read-only accessor VerifyChain needs - kept minimal and
// separate from Writer so verification can run against a plain
// (non-transactional) pool connection, unlike Write which requires an
// ambient transaction.
type Store interface {
	// ListEvents returns every AuditEvent for tenantID in ID (ULID, so
	// chronological) order.
	ListEvents(ctx context.Context, tenantID uuid.UUID) ([]AuditEvent, error)
}

// PostgresStore implements Store directly against a pool (no ambient-tx
// requirement - a read-only walk of already-committed rows).
type PostgresStore struct {
	Pool *pgxpool.Pool
}

func NewPostgresStore(pool *pgxpool.Pool) *PostgresStore {
	return &PostgresStore{Pool: pool}
}

func (s *PostgresStore) ListEvents(ctx context.Context, tenantID uuid.UUID) ([]AuditEvent, error) {
	rows, err := s.Pool.Query(ctx,
		`SELECT id, tenant_id, actor_id, actor_type, event_type, entity_type, entity_id, before_state, after_state, host(ip_address), request_id, occurred_at, hash_chain_prev
		 FROM audit_events WHERE tenant_id = $1 ORDER BY id`,
		tenantID,
	)
	if err != nil {
		return nil, fmt.Errorf("audit: list events: %w", err)
	}
	defer rows.Close()

	var out []AuditEvent
	for rows.Next() {
		var evt AuditEvent
		var actorID, ipAddress, requestID *string
		if err := rows.Scan(&evt.ID, &evt.TenantID, &actorID, &evt.ActorType, &evt.EventType, &evt.EntityType, &evt.EntityID,
			&evt.BeforeState, &evt.AfterState, &ipAddress, &requestID, &evt.OccurredAt, &evt.HashChainPrev); err != nil {
			return nil, fmt.Errorf("audit: list events: scan: %w", err)
		}
		if actorID != nil {
			evt.ActorID = *actorID
		}
		if ipAddress != nil {
			evt.IPAddress = *ipAddress
		}
		if requestID != nil {
			evt.RequestID = *requestID
		}
		out = append(out, evt)
	}
	return out, rows.Err()
}

// VerificationReport is VerifyChain's result - EventsChecked counts
// every event walked regardless of outcome; FirstBreakAt is nil for a
// clean chain, or the ID of the first event whose HashChainPrev doesn't
// match the recomputed hash of the event before it.
type VerificationReport struct {
	TenantID      uuid.UUID
	EventsChecked int
	FirstBreakAt  *string
}

// VerifyChain walks tenantID's events in ID order, recomputing each
// event's hash (ComputeHash(event, event.HashChainPrev)) and checking it
// against the NEXT event's stored HashChainPrev - since Hash itself isn't
// persisted (see event.go's doc comment on AuditEvent.Hash), this
// recomputation is the only way to detect tampering: any retroactive
// edit to event i's fields changes its recomputed hash, which then
// mismatches event i+1's already-fixed HashChainPrev.
func VerifyChain(ctx context.Context, store Store, tenantID uuid.UUID) (VerificationReport, error) {
	events, err := store.ListEvents(ctx, tenantID)
	if err != nil {
		return VerificationReport{}, err
	}

	report := VerificationReport{TenantID: tenantID}
	prevHash := ""
	for _, evt := range events {
		report.EventsChecked++

		if evt.HashChainPrev != prevHash {
			id := evt.ID
			report.FirstBreakAt = &id
			return report, nil
		}
		prevHash = ComputeHash(evt, evt.HashChainPrev)
	}
	return report, nil
}
