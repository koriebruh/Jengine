package audit

import (
	"context"
	"fmt"
	"time"

	"github.com/koriebruh/Jengine/internal/storage/postgres"
)

// Writer is the append-only audit log's write interface - synchronous,
// zero-loss-tolerance per plans/docs/10-observability-reliability.md
// §11.1's SLO table: a failed Write must propagate as an error to the
// caller (task 13's Transition, task 15's mutating handlers), never be
// logged-and-swallowed or made fire-and-forget.
type Writer interface {
	Write(ctx context.Context, evt AuditEvent) error
}

// PostgresWriter is the MVP Writer implementation. It reads/updates its
// ambient transaction from ctx (postgres.TxFromContext) rather than
// opening its own - callers (task 13) call this from inside the same
// transaction as the domain-state change being audited, so the two
// writes commit or roll back together atomically; a separate connection/
// transaction here would reintroduce exactly the dual-write risk the
// transactional-outbox pattern elsewhere in this codebase exists to
// avoid.
type PostgresWriter struct{}

func NewPostgresWriter() *PostgresWriter {
	return &PostgresWriter{}
}

// Write computes evt's hash against the tenant's current chain tail and
// inserts both the audit_events row and the updated chain-tail pointer.
//
// Concurrency control (plans/task/core/14 Implementation Notes): the
// INSERT ... ON CONFLICT DO UPDATE against audit_chain_tail's PK acts as
// an atomic "get current tail, creating the row if this is the tenant's
// first event, and lock it for the rest of this transaction" - Postgres
// takes a row-level lock during the UPDATE phase of an upsert, so a
// second concurrent Write for the SAME tenant blocks here until the
// first transaction commits or rolls back. Writes for DIFFERENT tenants
// never block each other (per-tenant chains, not one global chain - see
// event.go/ComputeHash's doc and plans/task/core/14 Common Pitfalls).
func (w *PostgresWriter) Write(ctx context.Context, evt AuditEvent) error {
	tx, ok := postgres.TxFromContext(ctx)
	if !ok {
		return fmt.Errorf("audit: no transaction in context - call within postgres.WithTx or tenancy.WithTenantTx")
	}

	if evt.ID == "" {
		evt.ID = NewULID()
	}
	if evt.OccurredAt.IsZero() {
		evt.OccurredAt = time.Now().UTC()
	}
	// Postgres timestamptz stores microsecond precision, Go's time.Now()
	// carries nanoseconds - hash against the SAME precision that will be
	// read back at verify time (plans/task/core/14 Common Pitfalls:
	// hashing a DB-generated timestamp with sub-millisecond
	// nondeterminism makes verification unreliable), or every event's
	// recomputed hash mismatches during VerifyChain purely from
	// nanosecond round-trip truncation, never from real tampering.
	evt.OccurredAt = evt.OccurredAt.Truncate(time.Microsecond)

	var prevHash string
	err := tx.QueryRow(ctx,
		`INSERT INTO audit_chain_tail (tenant_id, last_event_id, last_hash)
		 VALUES ($1, '', '')
		 ON CONFLICT (tenant_id) DO UPDATE SET tenant_id = EXCLUDED.tenant_id
		 RETURNING last_hash`,
		evt.TenantID,
	).Scan(&prevHash)
	if err != nil {
		return fmt.Errorf("audit: lock chain tail: %w", err)
	}

	hash := ComputeHash(evt, prevHash)

	_, err = tx.Exec(ctx,
		`INSERT INTO audit_events (id, tenant_id, actor_id, actor_type, event_type, entity_type, entity_id, before_state, after_state, ip_address, request_id, occurred_at, hash_chain_prev)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, NULLIF($10, '')::inet, $11, $12, $13)`,
		evt.ID, evt.TenantID, nullableText(evt.ActorID), evt.ActorType, evt.EventType, evt.EntityType, evt.EntityID,
		nullableJSON(evt.BeforeState), nullableJSON(evt.AfterState), evt.IPAddress, nullableText(evt.RequestID), evt.OccurredAt, prevHash,
	)
	if err != nil {
		return fmt.Errorf("audit: insert audit_events: %w", err)
	}

	_, err = tx.Exec(ctx,
		`UPDATE audit_chain_tail SET last_event_id = $1, last_hash = $2, updated_at = now() WHERE tenant_id = $3`,
		evt.ID, hash, evt.TenantID,
	)
	if err != nil {
		return fmt.Errorf("audit: update chain tail: %w", err)
	}

	return nil
}

func nullableText(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

func nullableJSON(b []byte) []byte {
	if len(b) == 0 {
		return nil
	}
	return b
}

var _ Writer = (*PostgresWriter)(nil)
