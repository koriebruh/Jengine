# Task 24: Full Multi-Tenancy Isolation Tiers

## Goal
Upgrade multi-tenancy from MVP's basic shared-RLS-only model (single-node Postgres) to the full three-tier model: Standard (shared, Citus-sharded, RLS defense-in-depth), Isolated Schema (dedicated schema per tenant in the shared Citus cluster), and Dedicated (separate cluster, dedicated Kafka topics/Redis namespace, optional dedicated k8s namespace). **Citus is introduced here for the first time** — MVP and local dev run plain single-node Postgres throughout (per `plans/docs/16-development-workflow.md` §16.2, an explicit dev-environment simplification); this is not an incremental tweak to existing tenancy code, it is a genuine infrastructure jump that changes how every distributed table is created and queried.

## Prerequisites
- Core task 04 (MVP tenancy context/RLS — this task extends its `TenantContext`/routing, doesn't replace the concept).
- Core task 03 (schema/migrations — distribution columns need to be added/verified on every table that will become Citus-distributed, including join tables that may not have had an obvious `tenant_id` before).

## Scope / Deliverables
- Migrations converting existing tables to Citus distributed tables (`SELECT create_distributed_table('transactions', 'tenant_id')`) and reference tables (`SELECT create_reference_table('currency_codes')` for currency codes, business calendars).
- `internal/tenancy/router.go` — upgraded `TenantRouter` resolving `tenant_id → {isolation_tier, cluster, schema, shard_key}` dynamically, replacing MVP's implicit single-shard assumption.
- Isolated-Schema tier provisioning: schema-per-tenant creation + migration fan-out support in the migration runner.
- Dedicated-tier provisioning: separate cluster reference wiring, dedicated Kafka topic naming (already anticipated in task 18's topic scheme), dedicated Redis namespace, optional k8s `NetworkPolicy`/namespace manifests.
- `deploy/docker-compose.citus.yml` (or a `--profile citus` addition to the existing compose file) — an **opt-in** local Citus stack for testing this task, following the same pattern as the existing `--profile observability` addition from task 02/16. The default dev stack stays plain single-node Postgres.
- Tenant-onboarding update: provisioning now actually stands up the chosen tier's infra (`ProvisionTenant(tier, region)`), not an implicit no-op.

## Design Reference
- `plans/docs/01-multi-tenancy.md` §2.1 (the three-tier table, and why not schema-per-tenant-for-everyone), §2.2 (routing), §2.3 (Tenant Registry DB — stays **unsharded**, do not distribute it).
- `plans/docs/08-storage-architecture.md` §9.1 (Citus distribution column = `tenant_id`, reference tables replicated, PgBouncer per shard).
- `plans/docs/11-scalability-roadmap.md` §12.1 (rebalance operations — `citus_rebalance_start` is an ops runbook item, not app code built here).
- `plans/docs/15-end-to-end-flows.md` §15.4 (tenant onboarding flow — this task makes step 2, "Tenant Router config populated," an actual working operation instead of a stub).

## Implementation Notes

### Router
```go
type IsolationTier string
const (
    TierStandard       IsolationTier = "standard"
    TierIsolatedSchema IsolationTier = "isolated_schema"
    TierDedicated      IsolationTier = "dedicated"
)

type TenantRouting struct {
    IsolationTier IsolationTier
    ClusterDSN    string // connection/pool reference for this tenant's target cluster
    SchemaName    string // set for IsolatedSchema tier, e.g. "tenant_<id>"
    ShardKey      string // tenant_id — the Citus distribution key for Standard tier
}

type TenantRouter interface {
    Resolve(ctx context.Context, tenantID string) (TenantRouting, error)
}
```
The repository layer must select connection pool + `search_path` based on `TenantRouting` before executing any query — extend the `WithTenantContext` helper from task 04 with a `WithTenantRouting(ctx, routing)` wrapper. The existing lint rule requiring an explicit non-nil `tenant_id` on every repository query (from task 04/01 §2.2) should be extended, not replaced, to also require routing resolution has happened.

### Standard tier
Existing RLS policies from task 04 stay in place as defense-in-depth — Citus distribution and RLS are complementary, not either/or. This task adds Citus sharding on top of the Standard tier; it does not remove or weaken RLS.

### Isolated Schema tier
Dedicated Postgres schema per tenant, within the same shared Citus cluster. The migration runner (task 03's tool) must support running the same migration set across N tenant schemas. This tier is opt-in for specific mid/large tenants, not the default — the "DDL-migration fan-out pain" risk the design explicitly calls out for schema-per-tenant-for-everyone is bounded here because it only applies to the subset of tenants who choose this tier.

### Dedicated tier
Separate Postgres cluster (Citus or plain, depending on the tenant's scale needs), dedicated Kafka topics (task 18's topic-naming scheme was already built dedicated-tier-aware — use it), dedicated Redis namespace, optional dedicated k8s namespace with `NetworkPolicy` manifests in `deploy/helm/`.

### Migrating existing MVP tenants
All pre-existing tenants ran implicitly on a single shared Postgres instance. The default upgrade path for them is Standard tier on the new Citus cluster:
- `create_distributed_table` works against already-populated tables, but sequence tables in dependency order and run during a maintenance window — this is a real data-migration operation, not a config flag flip.
- The **Tenant Registry DB stays unsharded** (per §2.3 — it is explicitly a small, independently-backed-up database, never distributed). Do not run `create_distributed_table` against it or its tables (`tenants`, `tenant_settings`, `tenant_isolation_config`, `tenant_quota`, `tenant_feature_flags`).
- Reference tables (currency codes, business/holiday calendars) use `create_reference_table` (replicated to all nodes), not `create_distributed_table` — using the wrong Citus table type breaks cross-shard joins against them.
- Every table intended for Citus distribution needs a `tenant_id` column for co-location, including pure join tables that may not have had one yet (e.g. verify `MatchResultLine` has `tenant_id`, not just foreign keys to tables that do) — add it via an additive migration if missing.

## Non-Goals / Guardrails
- Do not touch ClickHouse/analytics (task 22) or RBAC/OPA (task 23) in this task.
- Do not build Citus shard-rebalancing tooling beyond documenting the operational `citus_rebalance_start` command — that's an ops runbook, not application code.
- Do not build the Tenant Admin frontend's tier-picker UI — backend routing/provisioning only.
- Do not replace the default local-dev docker-compose stack with Citus — keep it plain single-node Postgres by default, exactly as task 02/16 set up; Citus is opt-in via a separate compose profile for testing this task.
- Do not distribute the Tenant Registry DB.

## Definition of Done
- Integration test (`testcontainers-go`, `citusdata/citus` image) creating distributed tables and verifying — via `EXPLAIN` — that a single-tenant query resolves to a single-shard query, not a scatter-gather across all shards. This is the single most important correctness assertion in this task: getting it wrong silently turns every query into a slow cross-shard scan.
- RLS + Citus interaction test confirming tenant isolation still holds under the distributed setup (tenant A cannot read tenant B's rows even via a raw distributed query).
- Schema-provisioning test for the Isolated Schema tier: create and tear down a tenant schema, run migrations against it.
- Routing-resolution unit tests covering all three tiers.
- Manual verification: `docker compose --profile citus up`, run the distribution migration against a seeded multi-tenant fixture, confirm query plans via `EXPLAIN` show single-shard access for a per-tenant query.

## Common Pitfalls
- Distributing the Tenant Registry DB itself — it must stay unsharded/global per the design.
- Using `create_distributed_table` for reference data (currency codes, calendars) instead of `create_reference_table`, breaking cross-shard joins.
- Treating Citus adoption as a pure configuration change — it requires schema changes (distribution column present on every distributed table, including join tables without an obvious `tenant_id`).
- Replacing the default local-dev Postgres compose service with Citus, breaking the deliberately lightweight default dev stack.
- Forgetting RLS policies must still apply after Citus distribution — Citus doesn't replace tenant isolation, it adds sharding on top of it.
- Attempting a single-cutover migration of all existing tenants without a maintenance window or dependency-ordered table sequencing.
