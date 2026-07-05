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
// ShardID/SchemaName/ClusterDSN are populated by TenantRouter.Resolve
// (plans/task/core/24, via WithTenantRouting) for tenants on the
// Isolated Schema/Dedicated tiers - empty for Standard tier, which uses
// the default connection pool (Citus's tenant_id sharding underneath
// is transparent to callers).
//
// UserID/Roles/BusinessUnit (plans/task/core/23) are the actor identity
// resolved from the JWT (empty for API-key auth, which carries no
// individual-user identity - see Middleware.resolveByAPIKey) - the
// source authz.Subject is built from for OPA evaluation.
type TenantContext struct {
	TenantID      uuid.UUID
	IsolationTier IsolationTier
	ShardID       string
	SchemaName    string
	ClusterDSN    string
	Region        string
	UserID        string
	Roles         []string
	BusinessUnit  string
}

// WithTenantRouting merges routing's resolved fields into tc and
// returns a context carrying the result - plans/task/core/24's own
// named extension point ("extend WithTenantContext... with a
// WithTenantRouting(ctx, routing) wrapper"). Call after WithTenant once
// TenantRouter.Resolve has run, typically right after tenancy.Middleware
// populates the base TenantContext from JWT/API-key claims.
func WithTenantRouting(ctx context.Context, tc TenantContext, routing TenantRouting) context.Context {
	tc.IsolationTier = routing.IsolationTier
	tc.SchemaName = routing.SchemaName
	tc.ClusterDSN = routing.ClusterDSN
	if routing.ShardKey != "" {
		tc.ShardID = routing.ShardKey
	}
	return WithTenant(ctx, tc)
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
