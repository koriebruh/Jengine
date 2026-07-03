> Part of the Jengine Reconciliation Engine design doc set. Index: [README.md](README.md) · Prev: [00-overview-and-architecture.md](00-overview-and-architecture.md)

# 01 — Multi-Tenancy Model

## 2.1 Isolation Strategy — Hybrid, Selectable Per Contract

| Tier | Isolation model | Target customer |
|---|---|---|
| Standard (shared pool) | Shared Citus cluster, row-level security (`tenant_id` on every table + Postgres RLS policies as defense-in-depth beyond app-layer filtering), Citus co-locates all of a tenant's tables on same shard group | SMB/mid-market banks, fintechs |
| Isolated Schema | Dedicated Postgres schema per tenant within shared Citus cluster | Mid-large tenants wanting logical isolation without dedicated infra cost |
| Dedicated (DB-per-tenant) | Separate Postgres cluster, dedicated Kafka topics/partitions, dedicated Redis namespace, optional dedicated k8s namespace + network policies | Large banks / regulated tenants needing physical isolation, data residency, BYOK |

**Why not pure schema-per-tenant for everyone** (likely ReconArt's legacy model): at 1000+ tenants, schema-per-tenant causes catalog bloat (Postgres planner/connection overhead), DDL-migration fan-out pain, connection-pooling complexity. Citus row-sharding on `tenant_id` handles the long tail efficiently while still offering hard isolation for enterprise tier — tiered model is itself a differentiator (isolation level = config choice, not architecture rewrite).

## 2.2 Tenant Identification & Routing
- Every request carries signed JWT with `tenant_id` claim (validated at Edge Gateway), or API key → `tenant_id` via Redis-cached lookup.
- `TenantContext` threaded through every internal call via Go `context.Context`; repository-layer queries require explicit non-nil `tenant_id` (enforced by lint rule + Postgres RLS as defense-in-depth).
- Tenant Router middleware resolves `tenant_id → {cluster/shard, schema, isolation_tier}` from a small dedicated Tenant Registry Postgres DB (replicated/backed up independently).

## 2.3 Tenant Config Storage
- Tenant Registry DB (unsharded): `tenants`, `tenant_settings`, `tenant_isolation_config`, `tenant_quota`, `tenant_feature_flags`.
- Rule/connector/workflow configs: tenant-scoped versioned JSONB in the tenant's own OLTP shard.
- Per-tenant DEK wrapped by platform/tenant KEK in KMS (AWS KMS / Vault Transit) — enables BYOK for Dedicated tier.

## 2.4 Resource Quotas & Noisy-Neighbor Mitigation
- Ingestion: per-tenant rate limits (Redis token-bucket).
- Matching compute: batch jobs via KEDA-scaled worker pools consuming tenant-partitioned Kafka topics or job queue (Asynq/River); bounded CPU/mem quota per job + weighted fair queuing across tenants, priority boost for SLA-bound tenants.
- Streaming: Kafka topics partitioned by `tenant_id` hash (or dedicated topics for Dedicated tier) — structural lag isolation.
- DB: PgBouncer per shard with per-tenant connection pool caps.
- Soft-throttle (HTTP 429 + `Retry-After`) before hard failure; per-tenant quota dashboards/alerts.

---
Next: [02-data-ingestion.md](02-data-ingestion.md)
