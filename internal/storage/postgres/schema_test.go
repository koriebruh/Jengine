package postgres_test

import (
	"context"
	"testing"
	"time"

	"github.com/koriebruh/Jengine/internal/testutil"
)

// Verifies plans/task/core/03's Definition of Done: migrations apply
// cleanly, expected tables/RLS exist, and RLS actually blocks cross-tenant
// reads at the DB layer - not just that policies exist on paper.

var tenantScopedTables = []string{
	"accounts", "connectors", "statements", "transactions",
	"match_rules", "match_results", "match_result_lines",
	"cases", "case_comments", "case_audit_events", "audit_events", "ingestion_dedup",
}

var allExpectedTables = append([]string{
	"tenants", "tenant_settings", "tenant_isolation_config", "tenant_quota", "tenant_feature_flags",
}, tenantScopedTables...)

func TestMigrations_SchemaAndRLS(t *testing.T) {
	if testing.Short() {
		t.Skip("requires Docker - skipped under -short; run make test-integration")
	}

	db := testutil.StartPostgres(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	t.Run("all expected tables exist", func(t *testing.T) {
		for _, table := range allExpectedTables {
			var exists bool
			err := db.Pool.QueryRow(ctx,
				`SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = $1)`,
				table,
			).Scan(&exists)
			if err != nil {
				t.Fatalf("query failed for table %q: %v", table, err)
			}
			if !exists {
				t.Errorf("expected table %q to exist, it does not", table)
			}
		}
	})

	t.Run("transactions has the documented multi-currency + status columns with correct types", func(t *testing.T) {
		type colSpec struct {
			name     string
			dataType string
		}
		want := []colSpec{
			{"amount", "numeric"},
			{"currency", "character"},
			{"fx_rate_to_base", "numeric"},
			{"base_amount", "numeric"},
			{"status", "text"},
			{"ingestion_idempotency_key", "text"},
			{"tenant_id", "uuid"},
		}
		for _, c := range want {
			var dataType string
			err := db.Pool.QueryRow(ctx,
				`SELECT data_type FROM information_schema.columns WHERE table_schema='public' AND table_name='transactions' AND column_name=$1`,
				c.name,
			).Scan(&dataType)
			if err != nil {
				t.Fatalf("column %q: query failed: %v", c.name, err)
			}
			if dataType != c.dataType {
				t.Errorf("column %q: expected data_type %q, got %q", c.name, c.dataType, dataType)
			}
		}
	})

	t.Run("audit_events.id is a ULID-shaped text PK, not uuid", func(t *testing.T) {
		var dataType string
		err := db.Pool.QueryRow(ctx,
			`SELECT data_type FROM information_schema.columns WHERE table_schema='public' AND table_name='audit_events' AND column_name='id'`,
		).Scan(&dataType)
		if err != nil {
			t.Fatalf("query failed: %v", err)
		}
		if dataType != "text" {
			t.Errorf("expected audit_events.id to be text (ULID), got %q", dataType)
		}
	})

	t.Run("every tenant-scoped table has RLS enabled and FORCED", func(t *testing.T) {
		for _, table := range tenantScopedTables {
			var enabled, forced bool
			err := db.Pool.QueryRow(ctx,
				`SELECT relrowsecurity, relforcerowsecurity FROM pg_class WHERE relname = $1 AND relnamespace = 'public'::regnamespace`,
				table,
			).Scan(&enabled, &forced)
			if err != nil {
				t.Fatalf("table %q: query failed: %v", table, err)
			}
			if !enabled {
				t.Errorf("table %q: expected ROW LEVEL SECURITY enabled, it is not", table)
			}
			if !forced {
				t.Errorf("table %q: expected FORCE ROW LEVEL SECURITY, it is not forced", table)
			}
		}
	})

	t.Run("RLS actually blocks cross-tenant reads via the jengine_app role", func(t *testing.T) {
		// The container's bootstrap user (db.Pool's connection) is a
		// superuser, which unconditionally bypasses RLS regardless of
		// FORCE ROW LEVEL SECURITY - see migrations/0001_init_schema.up.sql's
		// CREATE ROLE jengine_app comment. This sub-test must connect as
		// jengine_app, the non-superuser role the migration creates, or it
		// would trivially "pass" while proving nothing (this exact mistake
		// was made and caught manually during plans/task/core/03
		// verification against the docker-compose dev stack).
		appPool := testutil.AppRolePool(t, ctx, db.DSN)
		defer appPool.Close()

		const tenantA = "11111111-1111-1111-1111-111111111111"
		const tenantB = "22222222-2222-2222-2222-222222222222"

		_, err := appPool.Exec(ctx,
			`INSERT INTO tenants (id, name, isolation_tier, region, status) VALUES
			 ($1, 'Tenant A', 'STANDARD', 'us-east', 'ACTIVE'),
			 ($2, 'Tenant B', 'STANDARD', 'us-east', 'ACTIVE')`,
			tenantA, tenantB,
		)
		if err != nil {
			t.Fatalf("seeding tenants failed: %v", err)
		}

		_, err = appPool.Exec(ctx, `SET app.current_tenant_id = '`+tenantA+`'`)
		if err != nil {
			t.Fatalf("SET app.current_tenant_id (tenant A) failed: %v", err)
		}
		_, err = appPool.Exec(ctx,
			`INSERT INTO accounts (id, tenant_id, external_account_ref, account_type, currency, name)
			 VALUES ('33333333-3333-3333-3333-333333333333', $1, 'ACC-A', 'BANK', 'USD', 'Tenant A Account')`,
			tenantA,
		)
		if err != nil {
			t.Fatalf("inserting tenant A's account failed: %v", err)
		}

		var countAsA int
		if err := appPool.QueryRow(ctx, `SELECT count(*) FROM accounts`).Scan(&countAsA); err != nil {
			t.Fatalf("count as tenant A failed: %v", err)
		}
		if countAsA != 1 {
			t.Errorf("expected tenant A to see 1 row (their own account), got %d", countAsA)
		}

		_, err = appPool.Exec(ctx, `SET app.current_tenant_id = '`+tenantB+`'`)
		if err != nil {
			t.Fatalf("SET app.current_tenant_id (tenant B) failed: %v", err)
		}
		var countAsB int
		if err := appPool.QueryRow(ctx, `SELECT count(*) FROM accounts`).Scan(&countAsB); err != nil {
			t.Fatalf("count as tenant B failed: %v", err)
		}
		if countAsB != 0 {
			t.Errorf("RLS FAILED TO ISOLATE TENANTS: expected tenant B to see 0 rows (tenant A's data must not be visible), got %d", countAsB)
		}
	})
}
