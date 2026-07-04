package postgres

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/koriebruh/Jengine/internal/ingestion"
)

// OutboxRepo implements ingestion.OutboxWriter (tenant-scoped, via
// requireTx like every other repository in this package) and
// ingestion.OutboxReader (deliberately NOT tenant-scoped - see below).
type OutboxRepo struct {
	// superuserPool is used only by ListUnsent/MarkSent, which sweep
	// across ALL tenants. RLS's app.current_tenant_id session variable
	// can only ever represent one tenant per connection, so a
	// jengine_app connection structurally cannot serve a cross-tenant
	// query - this is not a workaround, it's the same reason migrations
	// run as the superuser role rather than jengine_app. Write, in
	// contrast, always runs inside a specific tenant's transaction (via
	// requireTx/WithTx) and never touches this pool.
	superuserPool *pgxpool.Pool
}

func NewOutboxRepo(superuserPool *pgxpool.Pool) *OutboxRepo {
	return &OutboxRepo{superuserPool: superuserPool}
}

func (r *OutboxRepo) Write(ctx context.Context, tenantID uuid.UUID, topic, key string, payload []byte) error {
	tx, err := requireTx(ctx, tenantID)
	if err != nil {
		return err
	}

	_, err = tx.Exec(ctx,
		`INSERT INTO ingestion_outbox (tenant_id, topic, key, payload) VALUES ($1, $2, $3, $4)`,
		tenantID, topic, key, payload,
	)
	if err != nil {
		return fmt.Errorf("postgres: OutboxRepo.Write: %w", err)
	}
	return nil
}

// ListUnsent sweeps unsent events across every tenant - see the
// superuserPool doc comment above for why this cannot go through the
// tenant-scoped requireTx path.
//
// tenantcheck:exempt - relay/poller infrastructure, sweeps all
// tenants' outbox rows by design; runs against the superuser pool, not
// jengine_app, so RLS is bypassed deliberately here, not defeated by
// accident.
func (r *OutboxRepo) ListUnsent(ctx context.Context, limit int) ([]ingestion.OutboxEvent, error) {
	rows, err := r.superuserPool.Query(ctx,
		`SELECT id, tenant_id, topic, key, payload, created_at, sent_at
		 FROM ingestion_outbox WHERE sent_at IS NULL ORDER BY created_at LIMIT $1`,
		limit,
	)
	if err != nil {
		return nil, fmt.Errorf("postgres: OutboxRepo.ListUnsent: %w", err)
	}
	defer rows.Close()

	var events []ingestion.OutboxEvent
	for rows.Next() {
		var e ingestion.OutboxEvent
		if err := rows.Scan(&e.ID, &e.TenantID, &e.Topic, &e.Key, &e.Payload, &e.CreatedAt, &e.SentAt); err != nil {
			return nil, fmt.Errorf("postgres: OutboxRepo.ListUnsent: scan: %w", err)
		}
		events = append(events, e)
	}
	return events, rows.Err()
}

// tenantcheck:exempt - see ListUnsent above; marking a specific
// already-identified outbox row sent doesn't need a tenant scope either.
func (r *OutboxRepo) MarkSent(ctx context.Context, id uuid.UUID) error {
	_, err := r.superuserPool.Exec(ctx, `UPDATE ingestion_outbox SET sent_at = now() WHERE id = $1`, id)
	if err != nil {
		return fmt.Errorf("postgres: OutboxRepo.MarkSent: %w", err)
	}
	return nil
}

var (
	_ ingestion.OutboxWriter = (*OutboxRepo)(nil)
	_ ingestion.OutboxReader = (*OutboxRepo)(nil)
)
