package apiserver

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/koriebruh/Jengine/internal/storage/postgres"
	"github.com/koriebruh/Jengine/internal/tenancy"
)

// PostgresIdempotencyStore is the Postgres-backed IdempotencyStore.
// idempotency_requests has RLS enabled like every other tenant-scoped
// table, so each Get/Save opens its own short transaction (via
// postgres.WithTx) purely to set app.current_tenant_id for that
// statement - deliberately NOT sharing the wrapped handler's own
// transaction (see WithIdempotency's doc comment on why a cache-write
// failure must never fail an already-succeeded request; a shared
// transaction would couple the two failure domains together).
type PostgresIdempotencyStore struct {
	Pool *pgxpool.Pool
}

func NewPostgresIdempotencyStore(pool *pgxpool.Pool) *PostgresIdempotencyStore {
	return &PostgresIdempotencyStore{Pool: pool}
}

func (s *PostgresIdempotencyStore) Get(ctx context.Context, tenantID uuid.UUID, key string) (StoredResponse, error) {
	var resp StoredResponse
	err := postgres.WithTx(tenancy.WithTenant(ctx, tenancy.TenantContext{TenantID: tenantID}), s.Pool, tenantID, func(ctx context.Context) error {
		tx, _ := postgres.TxFromContext(ctx)
		return tx.QueryRow(ctx,
			`SELECT request_hash, response_body FROM idempotency_requests WHERE tenant_id = $1 AND idempotency_key = $2`,
			tenantID, key,
		).Scan(&resp.RequestHash, &resp.ResponseBody)
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return StoredResponse{}, ErrIdempotencyKeyNotFound
	}
	if err != nil {
		return StoredResponse{}, fmt.Errorf("apiserver: idempotency store get: %w", err)
	}
	return resp, nil
}

func (s *PostgresIdempotencyStore) Save(ctx context.Context, tenantID uuid.UUID, key string, resp StoredResponse) error {
	err := postgres.WithTx(tenancy.WithTenant(ctx, tenancy.TenantContext{TenantID: tenantID}), s.Pool, tenantID, func(ctx context.Context) error {
		tx, _ := postgres.TxFromContext(ctx)
		_, err := tx.Exec(ctx,
			`INSERT INTO idempotency_requests (tenant_id, idempotency_key, request_hash, response_status, response_body)
			 VALUES ($1, $2, $3, 200, $4)
			 ON CONFLICT (tenant_id, idempotency_key) DO NOTHING`,
			tenantID, key, resp.RequestHash, resp.ResponseBody,
		)
		return err
	})
	if err != nil {
		return fmt.Errorf("apiserver: idempotency store save: %w", err)
	}
	return nil
}

var _ IdempotencyStore = (*PostgresIdempotencyStore)(nil)
