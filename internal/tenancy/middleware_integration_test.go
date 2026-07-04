package tenancy_test

import (
	"context"
	"net/url"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/koriebruh/Jengine/internal/tenancy"
	"github.com/koriebruh/Jengine/internal/testutil"
)

// appRolePoolSingleConn builds a pool connected as jengine_app (the
// non-superuser role migrations/0001_init_schema.up.sql creates - see
// internal/storage/postgres/schema_test.go for why the superuser
// connection must never be used to test RLS), capped at exactly one
// connection so every call in a test is guaranteed to reuse the same
// physical connection - the precondition for meaningfully testing
// pooled-connection leak behavior.
func appRolePoolSingleConn(t *testing.T, ctx context.Context, superuserDSN string) *pgxpool.Pool {
	t.Helper()

	u, err := url.Parse(superuserDSN)
	if err != nil {
		t.Fatalf("failed to parse superuser DSN: %v", err)
	}
	u.User = url.UserPassword("jengine_app", "jengine_app_dev")

	cfg, err := pgxpool.ParseConfig(u.String())
	if err != nil {
		t.Fatalf("failed to parse pool config: %v", err)
	}
	cfg.MaxConns = 1
	cfg.MinConns = 1

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatalf("failed to create single-conn jengine_app pool: %v", err)
	}
	return pool
}

func seedAccount(t *testing.T, ctx context.Context, db *testutil.TestDB, tenantID uuid.UUID, ref string) {
	t.Helper()
	_, err := db.Pool.Exec(ctx,
		`INSERT INTO accounts (id, tenant_id, external_account_ref, account_type, currency, name)
		 VALUES ($1, $2, $3, 'BANK', 'USD', $3)`,
		uuid.New(), tenantID, ref,
	)
	if err != nil {
		t.Fatalf("seed account failed: %v", err)
	}
}

// TestWithTenantTx_ActivatesRLS proves plans/task/core/04's Definition of
// Done: the application code (WithTenantTx), not just RLS policies in
// isolation (already proven by internal/storage/postgres/schema_test.go),
// correctly triggers tenant isolation end-to-end.
func TestWithTenantTx_ActivatesRLS(t *testing.T) {
	if testing.Short() {
		t.Skip("requires Docker - skipped under -short; run make test-integration")
	}

	db := testutil.StartPostgres(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	tenantA := seedTenant(t, ctx, db)
	tenantB := seedTenant(t, ctx, db)
	seedAccount(t, ctx, db, tenantA, "ACC-A")
	seedAccount(t, ctx, db, tenantB, "ACC-B")

	appPool := appRolePoolSingleConn(t, ctx, db.DSN)
	defer appPool.Close()

	ctxA := tenancy.WithTenant(ctx, tenancy.TenantContext{TenantID: tenantA})
	var countA int
	err := tenancy.WithTenantTx(ctxA, appPool, func(ctx context.Context, tx pgx.Tx) error {
		return tx.QueryRow(ctx, `SELECT count(*) FROM accounts`).Scan(&countA)
	})
	if err != nil {
		t.Fatalf("WithTenantTx (tenant A) failed: %v", err)
	}
	if countA != 1 {
		t.Errorf("expected tenant A to see exactly 1 account (its own), got %d", countA)
	}

	ctxB := tenancy.WithTenant(ctx, tenancy.TenantContext{TenantID: tenantB})
	var countB int
	err = tenancy.WithTenantTx(ctxB, appPool, func(ctx context.Context, tx pgx.Tx) error {
		return tx.QueryRow(ctx, `SELECT count(*) FROM accounts`).Scan(&countB)
	})
	if err != nil {
		t.Fatalf("WithTenantTx (tenant B) failed: %v", err)
	}
	if countB != 1 {
		t.Errorf("expected tenant B to see exactly 1 account (its own, not tenant A's), got %d", countB)
	}
}

// TestWithTenantTx_NoStaleSessionVariable is the concrete regression test
// for plans/task/core/04's single most dangerous pitfall: using a bare
// SET instead of SET LOCAL/set_config(...,true) would leave
// app.current_tenant_id set on the pooled connection after the
// transaction that set it ends, ready to leak into whatever unrelated
// code (or careless future change) next reuses that same physical
// connection outside of WithTenantTx. This test forces connection reuse
// (MaxConns=1) and checks the session variable directly, immediately
// after a committed transaction - not just that per-request query
// results happen to look right, which they would even with the buggy
// bare-SET version, since every WithTenantTx call resets the value at
// the start of its own transaction regardless.
func TestWithTenantTx_NoStaleSessionVariable(t *testing.T) {
	if testing.Short() {
		t.Skip("requires Docker - skipped under -short; run make test-integration")
	}

	db := testutil.StartPostgres(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	tenantA := seedTenant(t, ctx, db)

	appPool := appRolePoolSingleConn(t, ctx, db.DSN)
	defer appPool.Close()

	ctxA := tenancy.WithTenant(ctx, tenancy.TenantContext{TenantID: tenantA})
	err := tenancy.WithTenantTx(ctxA, appPool, func(ctx context.Context, tx pgx.Tx) error {
		var one int
		return tx.QueryRow(ctx, `SELECT 1`).Scan(&one)
	})
	if err != nil {
		t.Fatalf("WithTenantTx failed: %v", err)
	}

	// Same pool (MaxConns=1 guarantees the same physical connection),
	// but querying OUTSIDE of any WithTenantTx call - simulating any
	// code path that reuses a pooled connection without going through
	// this package's transaction helper. If app.current_tenant_id were
	// set via a bare SET, it would still show tenantA's ID here, after
	// the transaction that set it already committed.
	var current *string
	err = appPool.QueryRow(ctx, `SELECT current_setting('app.current_tenant_id', true)`).Scan(&current)
	if err != nil {
		t.Fatalf("current_setting query failed: %v", err)
	}
	if current != nil && *current != "" {
		t.Errorf("app.current_tenant_id leaked past its transaction: got %q, want unset (empty/NULL) - this means SET LOCAL/set_config(...,true) isn't actually transaction-scoped, check WithTenantTx", *current)
	}
}
