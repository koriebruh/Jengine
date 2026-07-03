> Part of the Jengine Reconciliation Engine design doc set. Index: [README.md](README.md) · Prev: [09-security-compliance.md](09-security-compliance.md)

# 10 — Observability & Reliability

## 11.1 SLOs (tunable per contract tier)

| Metric | Target |
|---|---|
| Streaming match p99 latency | < 1s ingestion → decision |
| Batch reconciliation (50M records) | < 30 min full tenant daily batch |
| API availability | 99.95% Standard, 99.99% Dedicated |
| Data durability | 11 nines (object storage + multi-AZ DB replication) |
| Audit log write durability | Zero loss tolerance — synchronous ack before API success |

## 11.2 Metrics/Tracing
- OpenTelemetry SDK in every service — traces span ingestion→normalization→matching→case creation→webhook delivery, single `trace_id` for "why didn't this match" debugging (major support-cost driver in this domain).
- Golden-signals dashboards per service + business-metric dashboards (match rate %, auto-match %, break aging, SLA compliance %) per tenant, exposed to tenant admins too (trust-building transparency).

## 11.3 Idempotent Replay/Reprocessing
- Idempotency keys + sufficient Kafka retention → any pipeline stage safely re-runnable (e.g. replay affected date range through corrected matching engine version without duplicate side effects).
- DLQ + manual redrive tooling per stage with full error context.

## 11.4 Disaster Recovery
- Postgres: continuous WAL archiving + periodic base backups (pgBackRest) to object storage, cross-region replication for Dedicated/DR-contracted tenants; RPO < 5min, RTO < 1hr Standard (tighter for Dedicated/DR tier).
- Kafka: RF=3 across AZs, replay buffer aids recovery.
- Regular DR game days (simulated region failure, restore drills) as operational practice.

## 11.5 Deployment Strategy
- Blue-green for Matching Engine + Case/Workflow services specifically (most correctness-sensitive) — shadow-traffic validation (compare old/new match outputs on live-mirrored traffic) before cutover.
- Canary rollout (5%→25%→100%) for API/ingestion layers, gated on error-rate/latency SLOs, auto-rollback via Argo Rollouts.
- DB migrations: strict expand-contract pattern (add nullable column → dual-write → backfill → cutover reads → drop old) — never single-step breaking migration at this scale.

---
Next: [11-scalability-roadmap.md](11-scalability-roadmap.md)
