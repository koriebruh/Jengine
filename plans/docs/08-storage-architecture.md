> Part of the Jengine Reconciliation Engine design doc set. Index: [README.md](README.md) · Prev: [07-api-extensibility.md](07-api-extensibility.md)

# 08 — Storage Architecture

## 9.1 OLTP: PostgreSQL + Citus
- Distribution column = `tenant_id` for Standard tier (co-locates tenant data, enables efficient in-tenant joins); reference tables (currency codes, business calendars) replicated to all nodes.
- Read replicas per shard for reporting-adjacent OLTP reads, isolating write-path latency.
- PgBouncer (transaction-pooling mode) per shard.

## 9.2 Analytical Store: ClickHouse
- Fed via Debezium CDC → Kafka → ClickHouse Kafka Engine tables → Materialized Views (`mv_breaks_daily_aging`, `mv_match_rate_by_rule`, `mv_sla_compliance`).
- Powers all dashboards/reports/GraphQL reporting gateway. **OLTP never queried directly for analytics** — keeps transactional hot path isolated from ad-hoc reporting load.

## 9.3 CDC & Sync Consistency
- Debezium (Postgres logical replication) captures row-level changes, publishes per-table Kafka topics feeding both ClickHouse ingestion and audit/webhook outbox — one CDC mechanism, multiple downstream consumers.
- Consistency model: ClickHouse eventually consistent (sub-second to few-seconds lag) — acceptable for analytics; anything needing strong consistency (case state, approval gates) reads OLTP directly.

## 9.4 Data Retention & Archival (7+ year compliance)
- Hot tier (Postgres/Citus): recent 12–18 months, fully queryable.
- Warm tier (ClickHouse): 2–3 years aggregated + detail, TTL-based tiered storage (`TTL ... TO VOLUME 'cold'`).
- Cold/archival: raw source files, full audit log, closed cases older than warm window → S3/object storage with Object Lock (WORM, compliance mode), partitioned by tenant/year/month, Parquet format (queryable via Athena/Trino/DuckDB without live-DB restore), retained per tenant jurisdiction (commonly 7yr, configurable).
- GDPR/right-to-erasure tension: financial audit retention generally overrides erasure for regulated records; where erasure applies (non-regulated PII), use field-level tokenization/redaction in archival records rather than deletion — redaction pass replaces PII with token while preserving audit hash-chain integrity (hash computed over canonical form including redaction marker, designed in from the start — see [09-security-compliance.md](09-security-compliance.md) §10.1).

---
Next: [09-security-compliance.md](09-security-compliance.md)
