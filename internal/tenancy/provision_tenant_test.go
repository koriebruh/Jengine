package tenancy_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/koriebruh/Jengine/internal/tenancy"
)

func TestProvisionTenant_Standard(t *testing.T) {
	if testing.Short() {
		t.Skip("requires Docker - skipped under -short; run make test-integration")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, localSuperuserDSNForProvisioning)
	if err != nil {
		t.Fatalf("connect to local dev Postgres: %v", err)
	}
	defer pool.Close()

	tenantID := uuid.New()
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), `DELETE FROM tenant_isolation_config WHERE tenant_id = $1`, tenantID)
		_, _ = pool.Exec(context.Background(), `DELETE FROM tenants WHERE id = $1`, tenantID)
	})

	err = tenancy.ProvisionTenant(ctx, pool, localSuperuserDSNForProvisioning, "../../migrations", tenantID,
		tenancy.ProvisionTenantParams{Name: "Standard Test Tenant", Region: "us-east", Tier: tenancy.IsolationTierStandard}, "")
	if err != nil {
		t.Fatalf("ProvisionTenant failed: %v", err)
	}

	var shardID string
	if err := pool.QueryRow(ctx, `SELECT shard_id FROM tenant_isolation_config WHERE tenant_id = $1`, tenantID).Scan(&shardID); err != nil {
		t.Fatalf("query isolation config failed: %v", err)
	}
	if shardID != tenantID.String() {
		t.Errorf("expected shard_id=%s, got %q", tenantID, shardID)
	}
}

func TestProvisionTenant_Dedicated(t *testing.T) {
	if testing.Short() {
		t.Skip("requires Docker - skipped under -short; run make test-integration")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, localSuperuserDSNForProvisioning)
	if err != nil {
		t.Fatalf("connect to local dev Postgres: %v", err)
	}
	defer pool.Close()

	tenantID := uuid.New()
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), `DELETE FROM tenant_isolation_config WHERE tenant_id = $1`, tenantID)
		_, _ = pool.Exec(context.Background(), `DELETE FROM tenants WHERE id = $1`, tenantID)
	})

	const clusterRef = "postgres://dedicated-cluster.internal/jengine"
	err = tenancy.ProvisionTenant(ctx, pool, localSuperuserDSNForProvisioning, "../../migrations", tenantID,
		tenancy.ProvisionTenantParams{Name: "Dedicated Test Tenant", Region: "eu-west", Tier: tenancy.IsolationTierDedicated}, clusterRef)
	if err != nil {
		t.Fatalf("ProvisionTenant failed: %v", err)
	}

	var gotClusterRef string
	if err := pool.QueryRow(ctx, `SELECT cluster_ref FROM tenant_isolation_config WHERE tenant_id = $1`, tenantID).Scan(&gotClusterRef); err != nil {
		t.Fatalf("query isolation config failed: %v", err)
	}
	if gotClusterRef != clusterRef {
		t.Errorf("expected cluster_ref=%q, got %q", clusterRef, gotClusterRef)
	}
}

// TestProvisionTenant_IsolatedSchema exercises the FULL router round
// trip: provision an Isolated Schema tenant, then confirm
// RegistryTenantRouter.Resolve reports the schema name the real
// provisioned schema actually has.
func TestProvisionTenant_IsolatedSchema_RouterRoundTrip(t *testing.T) {
	if testing.Short() {
		t.Skip("requires Docker - skipped under -short; run make test-integration")
	}
	requireMigrateCLI(t)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, localSuperuserDSNForProvisioning)
	if err != nil {
		t.Fatalf("connect to local dev Postgres: %v", err)
	}
	defer pool.Close()

	tenantID := uuid.New()
	t.Cleanup(func() {
		_ = tenancy.DeprovisionIsolatedSchema(context.Background(), pool, tenantID)
		_, _ = pool.Exec(context.Background(), `DELETE FROM tenant_isolation_config WHERE tenant_id = $1`, tenantID)
		_, _ = pool.Exec(context.Background(), `DELETE FROM tenants WHERE id = $1`, tenantID)
	})

	err = tenancy.ProvisionTenant(ctx, pool, localSuperuserDSNForProvisioning, "../../migrations", tenantID,
		tenancy.ProvisionTenantParams{Name: "Isolated Test Tenant", Region: "ap-south", Tier: tenancy.IsolationTierIsolated}, "")
	if err != nil {
		t.Fatalf("ProvisionTenant failed: %v", err)
	}

	registry := tenancy.NewPostgresRegistryRepo(pool)
	router := tenancy.NewRegistryTenantRouter(registry)

	routing, err := router.Resolve(ctx, tenantID.String())
	if err != nil {
		t.Fatalf("Resolve failed: %v", err)
	}
	wantSchema := tenancy.SchemaNameFor(tenantID)
	if routing.SchemaName != wantSchema {
		t.Errorf("expected router to resolve schema %q, got %q", wantSchema, routing.SchemaName)
	}
	if routing.IsolationTier != tenancy.IsolationTierIsolated {
		t.Errorf("expected Isolated tier, got %v", routing.IsolationTier)
	}
}
