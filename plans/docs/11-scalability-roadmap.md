> Part of the Jengine Reconciliation Engine design doc set. Index: [README.md](README.md) · Prev: [10-observability-reliability.md](10-observability-reliability.md)

# 11 — Scalability Plan for Growth

## 12.1 Scaling 1M → 50M+ Records/Day
- Ingestion: stateless connector pods, k8s HPA on CPU/queue-depth; file connectors parallelize per-file-per-worker.
- Kafka: over-provision partition count early (repartitioning existing data is hard; increasing count is easy) — e.g. 50–100 partitions per shard topic even at low initial volume.
- Matching workers: KEDA autoscaling on consumer lag (streaming) / job-queue depth (batch) — primary scaling lever, embarrassingly parallel at partition level (see [04-matching-engine.md](04-matching-engine.md) §5.2).
- Postgres/Citus: add worker nodes + rebalance shards (`citus_rebalance_start`) as volume grows; independent read replicas for reporting reads; split largest Dedicated tenants onto own coordinator group before shared-cluster impact.
- ClickHouse: add shards/replicas as historical volume grows; date partition-pruning keeps query perf stable into tens of billions of rows.
- Sharding evolution designed as infra operation, not app redesign, because `tenant_id`-based distribution chosen from day one.

## 12.2 Phased Roadmap

**Phase 0 — MVP** (prove core value, land design partners)
- Modular monolith (`coreapi`) + separate Ingestion Gateway + Batch Matching Engine.
- Connectors: CSV/Excel, SFTP, one MT940 parser.
- Rule engine: exact + tolerance + basic fuzzy (Jaro-Winkler), one-to-one + simple one-to-many.
- Case management: basic lifecycle (simple state machine + Postgres, no Temporal yet — upgrade path planned, not blocking), manual assignment, comment trail.
- Basic RLS multi-tenancy (defer full tiered isolation).
- Postgres only (defer ClickHouse; Postgres materialized views acceptable at MVP scale).
- Basic REST API, no webhooks/GraphQL yet.
- Frontend: thin internal UI only — Case Queue + Case Detail + connector status page, polling not SSE, no rule-builder UI (rules authored as raw YAML/JSON) — see [14-dashboard-frontend.md](14-dashboard-frontend.md) §14.4.
- Goal: validate matching quality + workflow UX with 2-3 design-partner banks, hundreds of thousands to low millions/day.

**Phase 1 — V1** (production multi-tenant launch)
- Full multi-tenancy tiering, Citus sharding live.
- Streaming matching engine + Kafka/Redpanda, hybrid batch/streaming model.
- Temporal-based case workflow: SLA timers, auto-assignment, maker-checker approvals.
- ClickHouse analytics + CDC pipeline; dashboards.
- Full connector set (BAI2, ISO20022, API pull, webhook receiver); Connector SDK v1 (native Go plugins, WASM sandbox fast-follow).
- Webhook system + audit hash-chaining + WORM archival.
- Frontend: full sellable screen set — rule builder + backtesting sandbox, tenant admin, webhook config UI, SSE-based live updates ([14-dashboard-frontend.md](14-dashboard-frontend.md) §14.2–14.4).
- Goal: sellable, compliant SaaS at 1M–10M records/day across dozens of tenants.

**Phase 2 — V2** (scale + differentiation deepening)
- ML-based match scoring + feedback loop; rule backtesting sandbox.
- GraphQL reporting gateway; connector marketplace opened to third parties.
- Multi-region deployment for data residency; matured DR automation (game days, automated failover).
- Validated scale-out to 50M+ records/day (chaos/load testing, partition/shard tuning from real telemetry).
- Advanced OPA ABAC policies, SOC2 Type II audit completed.
- Goal: enterprise-tier readiness, full competitive parity + differentiation vs ReconArt in RFP evaluations.

---
Next: [12-competitive-differentiation.md](12-competitive-differentiation.md)
