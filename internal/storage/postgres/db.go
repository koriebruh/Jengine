package postgres

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

// NewPool creates a pgxpool.Pool for dsn, pinging once to fail fast on a
// bad connection string rather than deferring the error to the first
// query. pgx (not database/sql + a generic driver) is the deliberate
// choice here - native Postgres type support (numeric, arrays, jsonb)
// with less adapter glue than database/sql, per plans/task/core/05
// Implementation Notes. Callers must connect as the jengine_app role
// (never the migration/superuser role) or RLS provides no protection at
// all - see migrations/0001_init_schema.up.sql and
// internal/storage/postgres/schema_test.go.
func NewPool(ctx context.Context, dsn string) (*pgxpool.Pool, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("postgres: new pool: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("postgres: ping: %w", err)
	}
	return pool, nil
}
