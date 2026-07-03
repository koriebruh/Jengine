# Task 04: Tenancy Context and Routing

## Goal
Build the foundational multi-tenancy safety layer: `TenantContext` propagation through Go's `context.Context`, the Tenant Registry data-access layer, HTTP/gRPC middleware that resolves and injects tenant identity per request, and the enforcement mechanism (lint + runtime guard) that makes it structurally hard for any repository query to run without an explicit tenant scope. Per plans/docs/13-implementation-notes.md, `internal/tenancy/context.go` is called out as "foundational for every other module's safety" — every task from 05 onward depends on this existing and being correct, since a bug here is a cross-tenant data leak in a financial system.

## Prerequisites
Task 03 (database schema — RLS policies and the `app.current_tenant_id` session-variable contract must already exist for this task's middleware to set correctly). Task 01 (repo skeleton).

## Scope / Deliverables
- `internal/tenancy/context.go` — `TenantContext` type, `WithTenant`/`TenantFromContext` context helpers.
- `internal/tenancy/registry.go` — Tenant Registry repository (reads/writes `tenants`, `tenant_settings`, `tenant_isolation_config`, `tenant_quota`, `tenant_feature_flags`).
- `internal/tenancy/middleware.go` — HTTP (and/or Connect-RPC interceptor, given task 15 will use Connect-RPC) middleware that resolves `tenant_id` from the request (JWT claim or API key lookup), loads routing info from the registry (with Redis-backed caching per plans/docs/01-multi-tenancy.md §2.2), injects `TenantContext` into `context.Context`, and sets the Postgres session variable the RLS policies from task 03 depend on.
- `internal/tenancy/quota.go` — per-tenant rate limiting (Redis token-bucket) per plans/docs/01-multi-tenancy.md §2.4 (basic implementation; full noisy-neighbor mitigation across matching/streaming is V1, see Non-Goals).
- `internal/platform/lint/tenantcheck/` (or extend task 01's `scripts/lint/check_tenant_id.sh` into a real `go/analysis`-based checker now that a real convention exists) — the enforcement mechanism proving every repository-layer function requires an explicit tenant argument.
- Unit + integration tests colocated under each new file.

## Design Reference
- plans/docs/01-multi-tenancy.md §2.2 (tenant identification & routing: JWT `tenant_id` claim or API-key → Redis-cached lookup; `TenantContext` via `context.Context`; repository queries require explicit non-nil `tenant_id`, enforced by lint + RLS as defense-in-depth; Tenant Router middleware resolves `tenant_id → {cluster/shard, schema, isolation_tier}` from the Tenant Registry DB) and §2.3 (registry table contents — already created by task 03; this task is the runtime access layer over them) and §2.4 (quotas: per-tenant Redis token-bucket rate limits, soft-throttle via 429+Retry-After before hard failure).
- plans/docs/00-overview-and-architecture.md §1.1 (module-boundary rule — `internal/tenancy` is imported by nearly everything; keep its own dependencies minimal so it never becomes the thing that couples unrelated modules together).
- plans/docs/09-security-compliance.md §10.3 (RBAC role names this middleware's auth layer will eventually need to carry — Tenant Admin, Recon Manager, Analyst, Approver, Auditor, API Integration Role — full RBAC/OPA wiring is task 23/V1; this task only needs to make sure `TenantContext` has room to carry an actor/role claim later without a breaking change, it does not implement RBAC itself).
- plans/docs/03-canonical-data-model.md (Tenant entity shape, already implemented in task 03's schema — this task's registry.go maps rows to Go structs, which task 05 formalizes further for domain-model entities generally; the Tenant struct itself can live in `internal/tenancy` since it's registry-owned, not `internal/domain`).

## Implementation Notes
- `TenantContext` struct (in `context.go`):
  ```go
  type TenantContext struct {
      TenantID       uuid.UUID
      IsolationTier  IsolationTier // enum: Standard | Isolated | Dedicated
      ShardID        string        // Citus shard/cluster ref, unused at MVP (single shard) but present so callers don't need a breaking change in V1
      SchemaName     string        // empty at MVP (Standard tier only)
      Region         string
  }

  type ctxKey struct{}

  func WithTenant(ctx context.Context, tc TenantContext) context.Context {
      return context.WithValue(ctx, ctxKey{}, tc)
  }

  // TenantFromContext returns the TenantContext and true if present. Callers
  // that require a tenant (nearly all repository code) should use
  // MustTenantFromContext and let it panic — a missing tenant in a
  // repository call is a programming error, not a recoverable condition.
  func TenantFromContext(ctx context.Context) (TenantContext, bool)
  func MustTenantFromContext(ctx context.Context) TenantContext // panics if absent
  ```
- Deliberate design call (explicit, since the docs don't spell out panic-vs-error here): use `MustTenantFromContext` inside repository implementations (task 05) so a missing tenant context is a loud programming bug caught in tests/staging, not a silent empty-tenant_id query slipping through — this directly serves §2.2's "no query without explicit tenant_id" requirement. Middleware-facing code (HTTP handlers) should use the non-panicking accessor and return 400/401 instead of crashing the process on a malformed request.
- `registry.go` repository interface:
  ```go
  type RegistryRepo interface {
      GetTenant(ctx context.Context, tenantID uuid.UUID) (Tenant, error)
      GetTenantByAPIKeyHash(ctx context.Context, apiKeyHash string) (Tenant, error)
      GetIsolationConfig(ctx context.Context, tenantID uuid.UUID) (IsolationConfig, error)
      GetQuota(ctx context.Context, tenantID uuid.UUID) (Quota, error)
      IsFeatureEnabled(ctx context.Context, tenantID uuid.UUID, flag string) (bool, error)
  }
  ```
  Note this repo intentionally does NOT take an additional `tenantID` "current caller" argument beyond the one being looked up — it is registry-scoped, not tenant-data-scoped, and is the one deliberate, documented exception to the "every repository query takes tenant_id" rule (it operates on the unsharded Tenant Registry DB, not per-tenant OLTP data). Document this exception explicitly in the lint tool's config/allowlist so it doesn't get flagged as a violation, and note it in a code comment so nobody "fixes" it into taking a redundant parameter.
- Middleware resolution flow: extract JWT from `Authorization: Bearer` header → validate signature/expiry → read `tenant_id` claim OR, for API-key auth, hash the presented key and call `GetTenantByAPIKeyHash` with a short-TTL Redis cache in front (per §2.2) → call `GetIsolationConfig` (also Redis-cached) → build `TenantContext` → `ctx = WithTenant(ctx, tc)` → for the DB connection used in this request/transaction, execute `SET LOCAL app.current_tenant_id = '<tenant_id>'` inside the transaction (must be `SET LOCAL`, scoped to the transaction, never a bare `SET` on a pooled connection — a pooled connection reused across tenants with a non-local `SET` is a cross-tenant leak bug). This is the concrete implementation of the session-variable contract task 03's RLS migration comment documents.
- Quota (`quota.go`): Redis token-bucket keyed `ratelimit:{tenant_id}:{resource}` (e.g. `ingestion`), basic `Allow(ctx, tenantID, resource) (bool, retryAfter time.Duration, error)`. Wire a simple HTTP middleware wrapper that returns `429` + `Retry-After` header on `Allow() == false`. Full weighted-fair-queuing across matching workers (§2.4's job-queue-level quota) is out of scope here — that's part of task 12 (batch worker) using this same quota primitive, not reimplementing it.
- Lint enforcement: implement as a `golang.org/x/tools/go/analysis`-based analyzer (preferred over task 01's shell/grep placeholder, now that a real function-signature convention exists) that flags any function in `internal/storage/postgres` (or any package task 05 designates as "repository layer") whose exported methods don't take a `context.Context` as first param and internally either call `tenancy.MustTenantFromContext` or take an explicit `tenantID uuid.UUID` parameter. Keep the allowlist for `internal/tenancy/registry.go` itself. Wire into `.golangci.yml` as a custom linter or a separate `make lint-tenancy` step invoked from CI (task 01's CI file gets this step filled in for real now).
- Concurrency: registry lookups must be safe for concurrent use (repo methods stateless, DB pool + Redis client are the only shared state — no package-level mutable maps without a mutex/sync primitive).

## Non-Goals / Guardrails
- Do not implement Citus shard routing logic, Isolated-Schema connection switching, or Dedicated-tier cluster routing — `ShardID`/`SchemaName` fields exist in the struct for forward-compatibility only; at MVP every tenant resolves to the single local Standard-tier Postgres instance. Full tiered routing is task 24 (V1).
- Do not implement RBAC/ABAC/OPA policy evaluation — `TenantContext` may eventually carry actor/role info but this task does not implement permission checks; that is task 23 (V1). Do not add a `Role` field speculatively beyond what's needed to avoid a breaking change; keep this task's scope to tenant identity/routing only.
- Do not implement the domain entity repositories themselves (Account, Transaction, etc.) — that is task 05. This task only builds the Tenant Registry repo and the context/middleware plumbing those other repos will depend on.
- Do not implement full weighted-fair-queuing job scheduling for matching workers — only the basic Redis token-bucket primitive; task 12 wires it into the batch worker's job dispatch.
- Do not build JWT issuance/login flows — assume JWTs are validated (signature/expiry check) but issued by a system outside this task's scope (or, if no auth provider exists yet, a minimal local dev JWT signer for testing is acceptable as a test fixture, not a production feature).

## Definition of Done
- Unit tests: `WithTenant`/`TenantFromContext`/`MustTenantFromContext` round-trip correctly; `MustTenantFromContext` panics (recovered in test) when context has no tenant.
- Integration test (`testcontainers-go` Postgres, per §16.4): two tenants seeded, middleware sets `SET LOCAL app.current_tenant_id` inside a transaction per request, and a query run through that transaction only sees its own tenant's rows — proving the middleware's session-variable wiring actually activates task 03's RLS policies end-to-end (not just that RLS exists in isolation, which task 03 already tested — this test proves the application code correctly triggers it).
- A test proves a pooled connection is never left with a stale `app.current_tenant_id` after a request completes (e.g. two sequential requests for different tenants reusing the same pooled connection each see only their own tenant's data) — this is the concrete regression test for the "must be SET LOCAL, not SET" pitfall.
- The tenancy lint analyzer has its own test suite proving it flags a violation fixture and passes a compliant one, and is wired into `make lint`/CI.
- Redis token-bucket quota test: rapid requests past the configured limit get `Allow() == false` with a sane `retryAfter`.
- Manual verification: hitting a locally-running stub endpoint (can be a minimal test-only HTTP handler wired for this task's verification only, not a production API surface — task 15 builds the real API) with a valid tenant JWT resolves and injects `TenantContext` correctly; an invalid/missing JWT is rejected before touching the DB.

## Common Pitfalls
- Using a bare `SET app.current_tenant_id = ...` instead of `SET LOCAL` inside a transaction — this is the single most dangerous mistake this task can make, since with connection pooling a non-local `SET` persists on the pooled connection and leaks tenant A's RLS context into tenant B's next query on the same connection. Every code review of this task should specifically check for this.
- Making `TenantFromContext` (or an equivalent) silently return a zero-value `TenantContext{}` instead of `(TenantContext{}, false)`/panic when absent — a zero-value UUID tenant_id that isn't caught is exactly the kind of "empty tenant_id slipping through" the design explicitly guards against (§2.2).
- Building out full Citus-aware shard routing logic now "since the struct has the fields" — resist; that's V1 (task 24), and building it now means building it twice (once against non-existent Citus infra, again for real once task 24 lands).
- Conflating the Tenant Registry repo's intentional exception (it looks up tenants by ID/API-key, so it can't itself take a "current tenant" parameter) with a general excuse to skip the tenant_id-required convention elsewhere — the exception is narrow and documented, not a precedent.
- Forgetting Redis caching on registry lookups (§2.2 explicitly calls for API-key → tenant_id via Redis-cached lookup) and hitting the Tenant Registry DB on every single request — this becomes a bottleneck and contradicts the design's stated caching strategy.
- Putting RBAC/permission logic into this task because "it's related to tenancy" — keep role/permission enforcement fully deferred to task 23; this task is identity/routing only.
