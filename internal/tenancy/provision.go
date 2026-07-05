package tenancy

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

// migrateBin resolves the `migrate` CLI the same way scripts/migrate.sh
// does: PATH first, falling back to $(go env GOPATH)/bin/migrate.
func migrateBin() string {
	if _, err := exec.LookPath("migrate"); err == nil {
		return "migrate"
	}
	if gopath, err := exec.Command("go", "env", "GOPATH").Output(); err == nil {
		candidate := filepath.Join(strings.TrimSpace(string(gopath)), "bin", "migrate")
		if _, err := os.Stat(candidate); err == nil {
			return candidate
		}
	}
	return "migrate"
}

// SchemaNameFor returns the Isolated Schema tier's Postgres schema name
// for tenantID - hyphens stripped since they aren't valid in an
// unquoted Postgres identifier. Matches tenant_isolation_config.schema_name's
// expected format (this is the one place that format is generated, so
// it's the single source of truth for it).
func SchemaNameFor(tenantID uuid.UUID) string {
	return "tenant_" + strings.ReplaceAll(tenantID.String(), "-", "")
}

// ProvisionIsolatedSchema stands up the Isolated Schema tier's actual
// infra for a tenant (plans/task/core/24): creates a dedicated Postgres
// schema and runs the full base migration set (migrations/*.sql)
// against it. Shells out to the `migrate` CLI (matching scripts/migrate.sh's
// own established pattern) rather than vendoring golang-migrate's Go
// library into the app binary - task 03's own explicit design
// constraint ("no ORM, no extra runtime dependency in the app binary")
// applies here too; this is an infrequent admin/provisioning operation,
// not a hot-path binary concern.
func ProvisionIsolatedSchema(ctx context.Context, pool *pgxpool.Pool, adminDSN string, tenantID uuid.UUID, migrationsPath string) error {
	schemaName := SchemaNameFor(tenantID)

	if _, err := pool.Exec(ctx, fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %q", schemaName)); err != nil {
		return fmt.Errorf("tenancy: create schema %q: %w", schemaName, err)
	}

	dsn := schemaScopedDSN(adminDSN, schemaName)
	cmd := exec.CommandContext(ctx, migrateBin(), "-path", migrationsPath, "-database", dsn, "up")
	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("tenancy: run migrations against schema %q: %w (output: %s)", schemaName, err, out)
	}

	// migrations/0001's own `GRANT USAGE ON SCHEMA public TO jengine_app`
	// is hardcoded to "public" (unqualified CREATE TABLE statements
	// elsewhering the migration set follow search_path correctly, but a
	// literal schema-qualified GRANT does not) - jengine_app needs the
	// same USAGE grant on THIS tenant's own schema to actually read/write
	// through it once application code connects with search_path scoped
	// to just this schema.
	if _, err := pool.Exec(ctx, fmt.Sprintf("GRANT USAGE ON SCHEMA %q TO jengine_app", schemaName)); err != nil {
		return fmt.Errorf("tenancy: grant schema usage on %q: %w", schemaName, err)
	}
	return nil
}

// ProvisionTenantParams is what ProvisionTenant needs beyond the tier -
// name/region for the tenants row itself.
type ProvisionTenantParams struct {
	Name   string
	Region string
	Tier   IsolationTier
}

// ProvisionTenant stands up whichever tier's actual infra a new tenant
// needs (plans/task/core/24 - "provisioning now actually stands up the
// chosen tier's infra... not an implicit no-op", replacing
// plans/docs/15-end-to-end-flows.md §15.4 step 2's previous stub).
//
//   - Standard: no extra infra - Citus's tenant_id-hash distribution
//     (this task's own migration) already places the new tenant's rows
//     correctly the moment they're inserted. shard_id is set to the
//     tenant's own id (matching task 18's dedicated-tier-aware topic-
//     naming scheme, `<event>.<shard_or_tenant_id>` - Standard-tier
//     Kafka topics use a shared shard bucket in production, but at the
//     scale this codebase actually runs at, tenant_id-as-shard is the
//     simplest correct value and callers needing a coarser shard
//     grouping can derive one from it).
//   - Isolated Schema: calls ProvisionIsolatedSchema (real DDL: a new
//     Postgres schema, migrated).
//   - Dedicated: persists the routing config (cluster_ref, dedicated
//     Kafka/Redis naming inputs) - does NOT stand up a second physical
//     Postgres cluster, Kafka broker, or Redis instance. That is real
//     infra/ops provisioning (a new k8s namespace, cluster credentials,
//     DNS) outside this codebase's Go binary, the same category this
//     task's own Non-Goals already carve out for Citus shard-rebalancing
//     and service-mesh installation. deploy/helm/dedicated-tenant/ has
//     the NetworkPolicy manifest template for when that infra step
//     happens; clusterRef here is where its resulting connection
//     reference gets recorded once it does.
func ProvisionTenant(ctx context.Context, pool *pgxpool.Pool, adminDSN, migrationsPath string, tenantID uuid.UUID, params ProvisionTenantParams, clusterRef string) error {
	_, err := pool.Exec(ctx,
		`INSERT INTO tenants (id, name, isolation_tier, region, status) VALUES ($1, $2, $3, $4, 'ACTIVE')`,
		tenantID, params.Name, params.Tier, params.Region,
	)
	if err != nil {
		return fmt.Errorf("tenancy: provision tenant: insert tenant row: %w", err)
	}

	shardID := tenantID.String()

	switch params.Tier {
	case IsolationTierStandard:
		_, err := pool.Exec(ctx,
			`INSERT INTO tenant_isolation_config (tenant_id, shard_id) VALUES ($1, $2)`,
			tenantID, shardID,
		)
		if err != nil {
			return fmt.Errorf("tenancy: provision tenant: insert isolation config: %w", err)
		}
		return nil

	case IsolationTierIsolated:
		if err := ProvisionIsolatedSchema(ctx, pool, adminDSN, tenantID, migrationsPath); err != nil {
			return fmt.Errorf("tenancy: provision tenant: %w", err)
		}
		_, err := pool.Exec(ctx,
			`INSERT INTO tenant_isolation_config (tenant_id, shard_id, schema_name) VALUES ($1, $2, $3)`,
			tenantID, shardID, SchemaNameFor(tenantID),
		)
		if err != nil {
			return fmt.Errorf("tenancy: provision tenant: insert isolation config: %w", err)
		}
		return nil

	case IsolationTierDedicated:
		_, err := pool.Exec(ctx,
			`INSERT INTO tenant_isolation_config (tenant_id, shard_id, cluster_ref) VALUES ($1, $2, $3)`,
			tenantID, shardID, clusterRef,
		)
		if err != nil {
			return fmt.Errorf("tenancy: provision tenant: insert isolation config: %w", err)
		}
		return nil

	default:
		return fmt.Errorf("tenancy: provision tenant: unrecognized isolation tier %q", params.Tier)
	}
}

// DeprovisionIsolatedSchema tears down a tenant's Isolated Schema tier
// schema entirely - CASCADE, since this is a genuine tenant-offboarding
// operation (all that tenant's data lived only in this schema).
func DeprovisionIsolatedSchema(ctx context.Context, pool *pgxpool.Pool, tenantID uuid.UUID) error {
	schemaName := SchemaNameFor(tenantID)
	if _, err := pool.Exec(ctx, fmt.Sprintf("DROP SCHEMA IF EXISTS %q CASCADE", schemaName)); err != nil {
		return fmt.Errorf("tenancy: drop schema %q: %w", schemaName, err)
	}
	return nil
}

// schemaScopedDSN appends search_path=schemaName plus a schema-specific
// x-migrations-table (golang-migrate's own version-tracking table must
// be per-schema - otherwise every tenant schema would share one
// version counter and desync immediately) to adminDSN.
func schemaScopedDSN(adminDSN, schemaName string) string {
	sep := "?"
	if strings.Contains(adminDSN, "?") {
		sep = "&"
	}
	return fmt.Sprintf("%s%ssearch_path=%s&x-migrations-table=schema_migrations_%s", adminDSN, sep, schemaName, schemaName)
}
