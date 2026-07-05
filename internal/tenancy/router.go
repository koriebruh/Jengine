package tenancy

import (
	"context"
	"fmt"

	"github.com/google/uuid"
)

// TenantRouting is where a tenant's data actually lives (plans/task/core/24) -
// resolved dynamically per request, replacing MVP's implicit single-
// shard assumption (every tenant resolving to the one local Standard-
// tier Postgres instance regardless of IsolationConfig's contents).
type TenantRouting struct {
	IsolationTier IsolationTier
	ClusterDSN    string // connection/pool reference for this tenant's target cluster - empty for Standard tier (uses the default pool)
	SchemaName    string // set for IsolatedSchema tier, e.g. "tenant_<id>"
	ShardKey      string // tenant_id itself - the Citus distribution key for Standard tier
}

// TenantRouter resolves a tenant's routing. The Standard tier's
// ClusterDSN is empty (repository code uses the default connection
// pool, Citus's tenant_id-based sharding is transparent below that) -
// Isolated Schema/Dedicated tiers carry enough for a caller to select a
// different pool/search_path.
type TenantRouter interface {
	Resolve(ctx context.Context, tenantID string) (TenantRouting, error)
}

// RegistryTenantRouter implements TenantRouter against the existing
// RegistryRepo (plans/task/core/04) - task 24 extends what that
// interface's data is USED for, it doesn't replace GetTenant/
// GetIsolationConfig themselves (tenant_isolation_config's shard_id/
// schema_name/cluster_ref columns already existed from task 04's own
// schema; this task is what actually makes routing decisions from
// them instead of every tenant implicitly landing on the one local
// Standard-tier instance).
type RegistryTenantRouter struct {
	Registry RegistryRepo
}

func NewRegistryTenantRouter(registry RegistryRepo) *RegistryTenantRouter {
	return &RegistryTenantRouter{Registry: registry}
}

func (r *RegistryTenantRouter) Resolve(ctx context.Context, tenantID string) (TenantRouting, error) {
	id, err := uuid.Parse(tenantID)
	if err != nil {
		return TenantRouting{}, fmt.Errorf("tenancy: resolve routing: invalid tenant id %q: %w", tenantID, err)
	}

	tenant, err := r.Registry.GetTenant(ctx, id)
	if err != nil {
		return TenantRouting{}, fmt.Errorf("tenancy: resolve routing: %w", err)
	}

	routing := TenantRouting{IsolationTier: tenant.IsolationTier, ShardKey: tenantID}

	switch tenant.IsolationTier {
	case IsolationTierStandard:
		// No isolation config row needed - Standard tier always uses
		// the default pool, sharded transparently by Citus underneath
		// on tenant_id (this task's own distribution migration).
		return routing, nil
	case IsolationTierIsolated, IsolationTierDedicated:
		cfg, err := r.Registry.GetIsolationConfig(ctx, id)
		if err != nil {
			return TenantRouting{}, fmt.Errorf("tenancy: resolve routing: load isolation config: %w", err)
		}
		routing.SchemaName = cfg.SchemaName
		routing.ClusterDSN = cfg.ClusterRef
		return routing, nil
	default:
		return TenantRouting{}, fmt.Errorf("tenancy: resolve routing: unrecognized isolation tier %q", tenant.IsolationTier)
	}
}
