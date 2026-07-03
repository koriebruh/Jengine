> Part of the Jengine Reconciliation Engine design doc set. Index: [README.md](README.md) · Prev: [11-scalability-roadmap.md](11-scalability-roadmap.md)

# 12 — Competitive Differentiation Summary

| ReconArt Weakness | Jengine Answer |
|---|---|
| Matching engine rigidity, slow batch-oriented tuning | Declarative versioned no-code rule DSL, pluggable fuzzy scoring, backtesting sandbox, partition-parallel Go engine hitting sub-second streaming + 30-min full-day batch at 50M/day (see [04-matching-engine.md](04-matching-engine.md)) |
| Exception/case management (ReconArt's strength — must match/beat) | Temporal-backed durable workflow: SLA timers, maker-checker, auto-assignment, hash-chained audit trail as platform primitives, OPA-driven ABAC customization (see [05-case-management.md](05-case-management.md)) |
| Batch-only, no real-time recon | Native Kafka/Redpanda streaming match with rolling-window candidates, unified with batch via explicit hybrid model — "continuous reconciliation" (see [06-streaming-architecture.md](06-streaming-architecture.md) §7.5) |
| Closed/legacy integration model | Contract-first gRPC/Connect-RPC (REST+gRPC+gRPC-Web from one def), full webhook catalog with HMAC signing + transparent redrive tooling, WASM-sandboxed third-party Connector SDK + certification marketplace (see [07-api-extensibility.md](07-api-extensibility.md)) |
| Rigid single-tenancy-only for large clients | Tiered isolation (shared RLS / isolated schema / dedicated cluster) selectable per contract, no architecture rewrite; region-pinning for residency (see [01-multi-tenancy.md](01-multi-tenancy.md)) |
| Opaque internals, hard to debug "why didn't this match" | End-to-end OTel tracing per transaction, transparent confidence-score breakdowns, tenant-visible health/SLA dashboards (see [10-observability-reliability.md](10-observability-reliability.md)) |

---
Next: [13-implementation-notes.md](13-implementation-notes.md)
