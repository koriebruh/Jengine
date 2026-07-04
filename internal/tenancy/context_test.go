package tenancy_test

import (
	"context"
	"testing"

	"github.com/google/uuid"

	"github.com/koriebruh/Jengine/internal/tenancy"
)

func TestWithTenant_RoundTrip(t *testing.T) {
	want := tenancy.TenantContext{
		TenantID:      uuid.New(),
		IsolationTier: tenancy.IsolationTierStandard,
		Region:        "us-east",
	}

	ctx := tenancy.WithTenant(context.Background(), want)

	got, ok := tenancy.TenantFromContext(ctx)
	if !ok {
		t.Fatal("expected TenantFromContext to find the tenant, got ok=false")
	}
	if got != want {
		t.Errorf("got %+v, want %+v", got, want)
	}
}

func TestTenantFromContext_Absent(t *testing.T) {
	_, ok := tenancy.TenantFromContext(context.Background())
	if ok {
		t.Fatal("expected ok=false for a context with no tenant, got true")
	}
}

func TestMustTenantFromContext_ReturnsWhenPresent(t *testing.T) {
	want := tenancy.TenantContext{TenantID: uuid.New()}
	ctx := tenancy.WithTenant(context.Background(), want)

	got := tenancy.MustTenantFromContext(ctx)
	if got != want {
		t.Errorf("got %+v, want %+v", got, want)
	}
}

func TestMustTenantFromContext_PanicsWhenAbsent(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected MustTenantFromContext to panic on a context with no tenant, it did not")
		}
	}()
	tenancy.MustTenantFromContext(context.Background())
}
