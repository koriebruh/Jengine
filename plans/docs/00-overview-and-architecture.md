> Part of the Jengine Reconciliation Engine design doc set. Index: [README.md](README.md)

# 00 — Overview & High-Level Architecture

## Executive Summary

Jengine: cloud-native, multi-tenant financial reconciliation platform in Go. Targets 1M–50M reconciled records/day per tenant-shard, sub-second matching latency for streaming events, 7+ year immutable audit retention (SOX/PCI-DSS/SOC2/GDPR-aware). Architected as modular monolith at MVP, decomposing into targeted microservices as scale demands.

## 1. High-Level Architecture

### 1.1 Microservices vs Modular Monolith — Decision

**Recommendation: modular monolith with hard module boundaries (few deployable binaries), extracting services along natural scaling seams.**

Rationale: premature microservices split creates distributed-systems tax before product-market fit. Go module boundaries (`internal/` visibility) give "microservices-in-waiting" without runtime cost.

Split into separate deployables from day one only where scaling/failure profile genuinely diverges (retrofitting later is expensive):
1. **Ingestion Gateway** (connectors, file watchers, Kafka producers) — bursty I/O, independent autoscaling.
2. **Matching Engine** (batch + streaming workers) — CPU/memory heavy, stateless, horizontally scaled.
3. **Case/Workflow Service** — transactional, latency-sensitive, strong consistency (backed by Temporal).
4. **API Gateway/BFF** — public-facing, independent rate-limit/WAF posture.
5. **Reporting/Analytics Service** — read-heavy, backed by ClickHouse, must never contend with OLTP.

Everything else (tenant mgmt, rule config CRUD, audit log writer, notification/webhook dispatch) lives in **Core Services monolith** (`coreapi`), Go modules under `internal/{tenancy,rules,cases,audit,notify}`, one binary, in-process calls until per-module load profile justifies extraction (extraction trigger = measured per-module RPS/CPU dashboards).

### 1.2 Service Topology (target V1)

```
                                   ┌─────────────────────────┐
                                   │   Edge / API Gateway     │
                                   │  (Envoy/Kong + WAF)      │
                                   └────────────┬─────────────┘
                                                │
                 ┌──────────────────────────────┼──────────────────────────────┐
                 │                              │                              │
        ┌────────▼────────┐          ┌──────────▼──────────┐         ┌─────────▼─────────┐
        │  REST/gRPC BFF   │          │   Webhook/Event      │         │  GraphQL Reporting │
        │  (public API)    │          │   Dispatcher         │         │  Gateway           │
        └────────┬─────────┘          └──────────┬───────────┘         └─────────┬──────────┘
                 │                                │                              │
   ┌─────────────┴──────────────────────────────────────────────────────┐        │
   │                     Core Services (modular monolith)               │        │
   │  ┌──────────┐ ┌───────────┐ ┌────────────┐ ┌───────────┐          │        │
   │  │ Tenancy  │ │Rule Config│ │ Case/Wkflw │ │Audit/Event│          │        │
   │  │ Service  │ │ Service   │ │ Service    │ │ Log Writer│          │        │
   │  └──────────┘ └───────────┘ └────────────┘ └───────────┘          │        │
   └───────────────────────┬──────────────────────────────────────────┘        │
                            │                                                   │
        ┌───────────────────┼────────────────────────┐                         │
        │                   │                        │                        │
┌───────▼────────┐  ┌───────▼─────────┐   ┌───────────▼──────────┐             │
│  Ingestion      │  │  Matching       │   │  Streaming Match     │             │
│  Gateway        │  │  Engine (Batch) │   │  Consumers (Kafka)   │             │
│  (Connectors)   │  │  (Worker Pool)  │   │                      │             │
└───────┬─────────┘  └───────┬─────────┘   └───────────┬──────────┘             │
        │                    │                          │                       │
        └──────────┬─────────┴──────────────┬───────────┘                       │
                    │                        │                                   │
             ┌──────▼──────┐         ┌───────▼────────┐                         │
             │  Kafka Bus  │◄────────┤  Debezium CDC  │                         │
             │ (Redpanda)  │         │  (OLTP→stream) │                         │
             └──────┬──────┘         └───────┬────────┘                         │
                    │                        │                                   │
        ┌───────────▼────────────┐  ┌────────▼─────────┐             ┌──────────▼─────────┐
        │  PostgreSQL/Citus      │  │  ClickHouse       │◄────────────┤  Reporting reads    │
        │  (OLTP, per-tenant     │  │  (analytics,      │             │  from CH, never     │
        │  sharded)              │  │  dashboards, CDC  │             │  hit OLTP           │
        └────────────────────────┘  │  sink)            │             └────────────────────┘
                                     └───────────────────┘
        ┌────────────────────────┐  ┌───────────────────┐
        │  Redis / KeyDB         │  │  Object Storage    │
        │  (caching, rate-limit, │  │  (S3/MinIO) - WORM │
        │  candidate index)      │  │  for source files, │
        └────────────────────────┘  │  archived audit log│
                                     └───────────────────┘
```

### 1.3 Tech Stack & Justification

| Layer | Choice | Why (vs alternatives) |
|---|---|---|
| Language | Go 1.23+ | Given constraint. Concurrency primitives map directly onto matching engine's parallel worker model; static binary simplifies multi-tenant deploy; strong gRPC/Protobuf tooling. |
| API framework | gRPC (internal) + Connect-RPC (grpc-web/REST bridge) + thin `net/http`/chi for REST/webhooks | Connect (bufbuild/connect-go): one `.proto` service serves gRPC, gRPC-Web, plain-JSON-over-HTTP — avoids maintaining separate REST/gRPC codepaths. |
| Message bus | Redpanda (Kafka-API-compatible) | No ZooKeeper/JVM, lower tail latency, drop-in Kafka protocol so Debezium/Schema Registry/franz-go/sarama all work unmodified. NATS JetStream considered (simpler ops, weaker CDC/schema-registry ecosystem). Pulsar rejected (BookKeeper ops overhead not justified at this scale). |
| OLTP database | PostgreSQL + Citus extension (row sharding) | Mature SQL, JSONB for rule configs, mature Debezium connector, native `tenant_id` distribution. CockroachDB considered (better native multi-region + serializable txns) but weaker Debezium/CDC maturity + higher write latency for OLTP hot path today — flagged as V2/V3 evaluation checkpoint if geo-distributed multi-region write becomes a hard requirement. Keep data-access layer behind repository interfaces to preserve migration option. |
| Analytical store | ClickHouse | Columnar, fast aggregation over billions of rows for dashboards. Fed via Kafka engine table + materialized views from CDC topic. Druid rejected (ops complexity); BigQuery/Snowflake rejected (external cloud dependency conflicts with on-prem-capable requirement common for banks). |
| CDC | Debezium (Postgres connector) → Kafka → ClickHouse + webhook/cache invalidation | Standard proven pattern; transactional outbox avoids dual-write problem. |
| Cache/rate-limit | Redis Cluster / KeyDB | Candidate-generation index cache for streaming match, distributed rate limiting, idempotency keys, pub/sub invalidation. |
| Fuzzy matching | Custom Go Jaro-Winkler/Levenshtein for field scoring + OpenSearch for cross-partition fuzzy text blocking assist | Native Go avoids network round-trip for majority of comparisons (hot path); OpenSearch used only as secondary "search assist" when blocking keys don't converge, never the primary match path. |
| Workflow orchestration | Temporal | Case lifecycle (open→investigating→pending-approval→resolved) with SLA timers, escalation, maker-checker approval gates is Temporal's exact sweet spot: durable timers, retries, human-in-the-loop signals, full audit history for free. Rejected: custom saga/state-machine (reinvents durable execution), cron+DB-flags (fragile, poor observability). |
| Object storage | S3-compatible (AWS S3 / MinIO for on-prem) with Object Lock (WORM) | Source files, reports, cold audit archives; Object Lock compliance mode satisfies WORM regulatory requirement. |
| Observability | OpenTelemetry → Prometheus + Tempo/Jaeger + Loki + Grafana | Vendor-neutral, standard CNCF stack, known quantity for enterprise security review. |
| Deployment | Kubernetes (EKS/GKE/on-prem) + Helm + ArgoCD (GitOps) | KEDA specifically for scaling Kafka-consumer matching workers on consumer-group lag. |
| Schema/contract | Protobuf everywhere (APIs + Kafka events) via Buf schema registry | One source of truth for API and event contracts; `buf breaking` in CI enforces compatibility. Confluent Schema Registry (Protobuf serde) still used at Kafka layer for registry enforcement. |

---
Next: [01-multi-tenancy.md](01-multi-tenancy.md)
