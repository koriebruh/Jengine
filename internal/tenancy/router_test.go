package tenancy_test

import (
	"context"
	"testing"

	"github.com/google/uuid"

	"github.com/koriebruh/Jengine/internal/tenancy"
)

type fakeRegistryForRouting struct {
	tenant tenancy.Tenant
	config tenancy.IsolationConfig
	cfgErr error
}

func (f *fakeRegistryForRouting) GetTenant(ctx context.Context, tenantID uuid.UUID) (tenancy.Tenant, error) {
	return f.tenant, nil
}
func (f *fakeRegistryForRouting) GetTenantByAPIKeyHash(ctx context.Context, hash string) (tenancy.Tenant, error) {
	return tenancy.Tenant{}, nil
}
func (f *fakeRegistryForRouting) GetIsolationConfig(ctx context.Context, tenantID uuid.UUID) (tenancy.IsolationConfig, error) {
	return f.config, f.cfgErr
}
func (f *fakeRegistryForRouting) GetQuota(ctx context.Context, tenantID uuid.UUID) (tenancy.Quota, error) {
	return tenancy.Quota{}, nil
}
func (f *fakeRegistryForRouting) IsFeatureEnabled(ctx context.Context, tenantID uuid.UUID, flag string) (bool, error) {
	return false, nil
}

func TestRegistryTenantRouter_Standard(t *testing.T) {
	tenantID := uuid.New()
	registry := &fakeRegistryForRouting{tenant: tenancy.Tenant{ID: tenantID, IsolationTier: tenancy.IsolationTierStandard}}
	router := tenancy.NewRegistryTenantRouter(registry)

	routing, err := router.Resolve(context.Background(), tenantID.String())
	if err != nil {
		t.Fatalf("Resolve failed: %v", err)
	}
	if routing.IsolationTier != tenancy.IsolationTierStandard {
		t.Errorf("expected Standard tier, got %v", routing.IsolationTier)
	}
	if routing.SchemaName != "" || routing.ClusterDSN != "" {
		t.Errorf("expected no schema/cluster for Standard tier, got %+v", routing)
	}
	if routing.ShardKey != tenantID.String() {
		t.Errorf("expected ShardKey=%s, got %s", tenantID, routing.ShardKey)
	}
}

func TestRegistryTenantRouter_IsolatedSchema(t *testing.T) {
	tenantID := uuid.New()
	registry := &fakeRegistryForRouting{
		tenant: tenancy.Tenant{ID: tenantID, IsolationTier: tenancy.IsolationTierIsolated},
		config: tenancy.IsolationConfig{TenantID: tenantID, SchemaName: "tenant_" + tenantID.String()},
	}
	router := tenancy.NewRegistryTenantRouter(registry)

	routing, err := router.Resolve(context.Background(), tenantID.String())
	if err != nil {
		t.Fatalf("Resolve failed: %v", err)
	}
	if routing.IsolationTier != tenancy.IsolationTierIsolated {
		t.Errorf("expected Isolated tier, got %v", routing.IsolationTier)
	}
	if routing.SchemaName != "tenant_"+tenantID.String() {
		t.Errorf("expected schema name to be resolved, got %q", routing.SchemaName)
	}
}

func TestRegistryTenantRouter_Dedicated(t *testing.T) {
	tenantID := uuid.New()
	registry := &fakeRegistryForRouting{
		tenant: tenancy.Tenant{ID: tenantID, IsolationTier: tenancy.IsolationTierDedicated},
		config: tenancy.IsolationConfig{TenantID: tenantID, ClusterRef: "postgres://dedicated-cluster/jengine"},
	}
	router := tenancy.NewRegistryTenantRouter(registry)

	routing, err := router.Resolve(context.Background(), tenantID.String())
	if err != nil {
		t.Fatalf("Resolve failed: %v", err)
	}
	if routing.IsolationTier != tenancy.IsolationTierDedicated {
		t.Errorf("expected Dedicated tier, got %v", routing.IsolationTier)
	}
	if routing.ClusterDSN != "postgres://dedicated-cluster/jengine" {
		t.Errorf("expected cluster DSN to be resolved, got %q", routing.ClusterDSN)
	}
}

func TestRegistryTenantRouter_InvalidTenantID(t *testing.T) {
	router := tenancy.NewRegistryTenantRouter(&fakeRegistryForRouting{})
	if _, err := router.Resolve(context.Background(), "not-a-uuid"); err == nil {
		t.Fatal("expected an error for a malformed tenant id")
	}
}
