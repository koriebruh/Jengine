package postgres

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/koriebruh/Jengine/internal/tenancy"
)

type txKey struct{}

// WithTx begins a transaction against pool, sets app.current_tenant_id
// for the transaction's lifetime only (set_config(...,true) - SET LOCAL
// semantics, never a bare SET; see internal/tenancy.WithTenantTx's doc
// comment for why a bare SET leaks across pooled-connection reuse), runs
// fn with the transaction available via TxFromContext, and
// commits/rolls back based on fn's return value.
//
// This is the non-HTTP-request counterpart to
// internal/tenancy.WithTenantTx - used by background workers/batch jobs
// (plans/task/core/12) that don't have an inbound HTTP request to derive
// the transaction's tenant scope from, so tenantID is passed explicitly
// here rather than pulled from ctx. Callers should still put a
// TenantContext into ctx first (tenancy.WithTenant) before calling this,
// since repository methods assert tenantID against it as a defensive
// check independent of RLS - see requireTx.
func WithTx(ctx context.Context, pool *pgxpool.Pool, tenantID uuid.UUID, fn func(ctx context.Context) error) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("postgres: begin tx: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck // no-op if already committed

	if _, err := tx.Exec(ctx, `SELECT set_config('app.current_tenant_id', $1, true)`, tenantID.String()); err != nil {
		return fmt.Errorf("postgres: set_config app.current_tenant_id: %w", err)
	}

	if err := fn(ContextWithTx(ctx, tx)); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// ContextWithTx returns a copy of ctx carrying an already-open
// transaction, for bridging tenancy.WithTenantTx's tx-as-parameter
// calling convention (used by HTTP-request code paths, where
// tenancy.Middleware + tenancy.WithTenantTx already opened the
// transaction and set app.current_tenant_id) into the same
// TxFromContext mechanism WithTx uses for background workers -
// repository methods don't need two code paths to find the transaction.
func ContextWithTx(ctx context.Context, tx pgx.Tx) context.Context {
	return context.WithValue(ctx, txKey{}, tx)
}

// TxFromContext returns the transaction set by WithTx/ContextWithTx, if
// any.
func TxFromContext(ctx context.Context) (pgx.Tx, bool) {
	tx, ok := ctx.Value(txKey{}).(pgx.Tx)
	return tx, ok
}

// requireTx is the shared entry-point every repository method calls
// first: it asserts tenantID (the explicit parameter every method takes)
// matches the TenantContext already in ctx - the defensive equality
// check from plans/task/core/05 Implementation Notes, independent of the
// RLS layer - and returns the ambient transaction those checks require
// to exist. Panics via tenancy.MustTenantFromContext if ctx has no
// tenant at all (a programming error: every code path reaching a
// repository method must have gone through tenancy.Middleware or
// manually called tenancy.WithTenant first).
func requireTx(ctx context.Context, tenantID uuid.UUID) (pgx.Tx, error) {
	tc := tenancy.MustTenantFromContext(ctx)
	if tc.TenantID != tenantID {
		return nil, fmt.Errorf("postgres: tenantID parameter %s does not match context tenant %s", tenantID, tc.TenantID)
	}
	tx, ok := TxFromContext(ctx)
	if !ok {
		return nil, fmt.Errorf("postgres: no transaction in context - call within postgres.WithTx or tenancy.WithTenantTx")
	}
	return tx, nil
}
