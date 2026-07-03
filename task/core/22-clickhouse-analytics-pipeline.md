# Task 22: ClickHouse Analytics Pipeline

## Goal
Stand up ClickHouse as the analytical store and wire the CDC pipeline that feeds it: Debezium (Postgres logical replication) → Kafka → ClickHouse Kafka Engine tables → materialized views. This is what lets dashboards and reports run without ever touching the OLTP hot path — a hard requirement, not an optimization, per the storage architecture doc. Delivers the three named materialized views the frontend Overview Dashboard (a separate frontend task) will query: `mv_breaks_daily_aging`, `mv_match_rate_by_rule`, `mv_sla_compliance`.

## Prerequisites
- Core task 18 (Kafka/Debezium/Kafka Connect already stood up there for the outbox pattern — this task **extends the same Kafka Connect cluster** with additional table-level CDC connectors, it does not stand up a second one).
- Core task 03 (schema — the source tables being CDC'd: `transactions`, `match_results`, and whatever the case/break tables are named after tasks 13/20).

## Scope / Deliverables
- `deploy/clickhouse/` — docker-compose service for local dev, Helm chart values for prod.
- `internal/storage/clickhouse/` — query layer (repo layout already names this package).
- SQL DDL: Kafka Engine tables, target `MergeTree`/`AggregatingMergeTree` tables, and the three materialized views.
- Debezium connector configs (in `deploy/redpanda/` or wherever task 18 put connector configs) for `transactions`, `match_results`, and case/break tables.
- Repointing the reporting/dashboard read endpoints from any MVP-era Postgres aggregate queries to ClickHouse-backed queries, **preserving the existing API route contract** so the frontend client doesn't need to change.

## Design Reference
- `plans/docs/08-storage-architecture.md` §9.2 (ClickHouse, Kafka Engine tables, the three named MVs), §9.3 (CDC/consistency model — ClickHouse is eventually consistent, sub-second-to-few-seconds lag, that's acceptable here), §9.4 (retention/TTL tiering — apply the warm-tier TTL config to these tables now, don't defer it).
- `plans/docs/05-case-management.md` §6.3 (SLA dashboards — aging buckets, breach rate, MTTR — is what `mv_sla_compliance` and `mv_breaks_daily_aging` must support).
- `plans/docs/14-dashboard-frontend.md` §14.2 screen 1 (the frontend consumer of these views — match its expected shape even though building that screen is not this task's job).

## Implementation Notes

### Kafka Engine table pattern
```sql
CREATE TABLE cdc_transactions_kafka (
  -- fields matching the Debezium envelope: op, before, after (nested), ts_ms, source
  op String,
  after String, -- JSON string; parse fields out in the MV's SELECT
  ts_ms UInt64
) ENGINE = Kafka
SETTINGS kafka_broker_list = 'redpanda:9092',
         kafka_topic_list = 'postgres.public.transactions',
         kafka_group_name = 'clickhouse_cdc',
         kafka_format = 'JSONEachRow';

CREATE MATERIALIZED VIEW cdc_transactions_mv TO transactions_local AS
SELECT JSONExtract(after, ...) AS ...
FROM cdc_transactions_kafka
WHERE op != 'd';
```
Handle Debezium's `op` codes (`c`=create, `u`=update, `d`=delete, `r`=snapshot read) explicitly — an update must not create a duplicate row in the target table. Use `ReplacingMergeTree` (keyed by primary key + a version/`ts_ms` column) for tables that need update-in-place semantics rather than plain `MergeTree`, which would otherwise accumulate every version as a separate row.

### The three materialized views
Two of these are naturally **event-triggered incremental** MVs; one is not, and treating all three the same way is the most likely mistake here.

- **`mv_match_rate_by_rule`** — incremental, insert-triggered. Source: `match_results` CDC stream. Aggregates count/avg-confidence by `(tenant_id, rule_id, day, status)` using `AggregatingMergeTree` + state functions (`countState`, `avgState`), queried with `-Merge` combinators. Straightforward — a `MatchResult` row's classification doesn't change relative to "now."
- **`mv_sla_compliance`** — also naturally incremental. Breach/on-time determination (`resolved_at` vs `sla_due_at`) is computed once, at case-resolution time, from data already in the CDC event — it doesn't depend on the current wall-clock. Aggregate breach rate/MTTR by `(tenant_id, team, root_cause_category, day)`.
- **`mv_breaks_daily_aging`** — **not** a simple insert-triggered MV. Aging bucket (0–1d, 1–3d, 3–7d, 7–30d, 30+d open) is relative to "now," which changes independent of any insert event — an open case's aging bucket must move forward even with zero new events. Implement this as a **ClickHouse refreshable materialized view** (`CREATE MATERIALIZED VIEW ... REFRESH EVERY 1 HOUR AS SELECT ...` — recomputing the aggregation from the underlying detail table on a schedule) rather than a plain insert-triggered MV. Document this distinction clearly in code comments so a later change doesn't "simplify" it into an incremental MV and silently break aging accuracy.

### Reporting endpoint cutover
If MVP-era reporting endpoints exist backed by ad-hoc Postgres aggregate queries (per `plans/docs/11-scalability-roadmap.md` §12.2 Phase 0, "Postgres materialized views acceptable at MVP scale"), repoint their implementation to query ClickHouse via `internal/storage/clickhouse/`, keeping the same route/response shape the frontend already expects — apply the same "don't break existing callers" discipline used in task 20's Temporal cutover and task 24's Citus cutover.

## Non-Goals / Guardrails
- Do not build the frontend Overview Dashboard screen — only the query layer/API it consumes.
- Do not build the GraphQL reporting gateway (§8.4) — V2, see task 26.
- Do not have any code path query OLTP Postgres directly for analytics/dashboard purposes after this task lands — that's the exact anti-pattern the design forbids ("OLTP never queried directly for analytics").
- Do not stand up a second Kafka Connect deployment — extend task 18's.
- Do not skip the TTL/tiered-storage configuration "for now" — configure it as part of this task, not a follow-up.

## Definition of Done
- Integration test (`testcontainers-go`: Postgres + Debezium/Kafka Connect + Redpanda + ClickHouse) inserting a `Transaction` row in Postgres and asserting it appears in the corresponding ClickHouse table within a bounded time window.
- MV correctness tests: load a known fixture dataset, query each MV, assert the aggregates match hand-computed expected values (golden-dataset style, consistent with the matching engine's testing philosophy).
- A test specifically exercising the refreshable-MV behavior for `mv_breaks_daily_aging` (e.g. manually trigger `SYSTEM REFRESH VIEW` in test and assert the aging bucket for a fixture open case has moved as time-simulated "now" advances).
- A test exercising Debezium update/delete tombstone handling, asserting no duplicate/stale rows accumulate in the target tables.

## Common Pitfalls
- Using a plain insert-triggered MV for `mv_breaks_daily_aging` — it will compute aging relative to insert time, not current time, and silently produce wrong aging buckets for cases with no recent events.
- Not handling Debezium `op='u'`/`op='d'` correctly, leaving duplicate or stale rows in target tables (use `ReplacingMergeTree` or explicit filtering, not plain `MergeTree`).
- Any dashboard/report code path falling back to querying Postgres "just for this one case" — forbidden by design, not a pragmatic shortcut.
- Skipping TTL/tiered-storage config, leaving unbounded hot-tier growth in ClickHouse.
- Standing up a separate Kafka Connect cluster instead of extending task 18's.
