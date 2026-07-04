package tenancy

import (
	"context"

	"github.com/google/uuid"
)

// IsolationTier is which multi-tenancy model a tenant uses. See
// plans/docs/01-multi-tenancy.md §2.1.
type IsolationTier string

const (
	IsolationTierStandard  IsolationTier = "STANDARD"
	IsolationTierIsolated  IsolationTier = "ISOLATED"
	IsolationTierDedicated IsolationTier = "DEDICATED"
)

// TenantContext carries per-request tenant identity and routing info.
// ShardID/SchemaName are present for forward-compatibility with V1 tiered
// routing (plans/task/core/24) but unused at MVP - every tenant resolves
// to the single local Standard-tier Postgres instance.
type TenantContext struct {
	TenantID      uuid.UUID
	IsolationTier IsolationTier
	ShardID       string
	SchemaName    string
	Region        string
}

type ctxKey struct{}

// WithTenant returns a copy of ctx carrying tc.
func WithTenant(ctx context.Context, tc TenantContext) context.Context {
	return context.WithValue(ctx, ctxKey{}, tc)
}

// TenantFromContext returns the TenantContext and true if present, or the
// zero value and false if not - callers that can meaningfully handle a
// missing tenant (HTTP handlers, which should respond 400/401) must use
// this, not MustTenantFromContext.
func TenantFromContext(ctx context.Context) (TenantContext, bool) {
	tc, ok := ctx.Value(ctxKey{}).(TenantContext)
	return tc, ok
}

// MustTenantFromContext returns the TenantContext or panics if absent.
// Repository-layer code (plans/task/core/05) must use this: a missing
// tenant context at that layer is a programming error (a query about to
// run without an explicit tenant scope), not a recoverable condition, and
// should fail loudly in tests/staging rather than silently querying with
// an empty tenant_id - see plans/docs/01-multi-tenancy.md §2.2.
func MustTenantFromContext(ctx context.Context) TenantContext {
	tc, ok := TenantFromContext(ctx)
	if !ok {
		panic("tenancy: no TenantContext in context - every repository call must run within a tenant-scoped context")
	}
	return tc
}
