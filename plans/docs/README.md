# Jengine — Reconciliation Engine Design Doc Set

Codename **Jengine**: production-grade, multi-tenant financial reconciliation engine (Go), designed to beat ReconArt. Greenfield design — no existing code in this repo yet.

Full context/scope also lives in `/home/jamalkya/.claude/plans/gw-ingin-bantu-gw-compressed-cupcake.md` (the original single-file plan this doc set was split from).

## Scope Recap
- Domain: financial/banking transaction reconciliation (bank statements vs GL/ledger, payment gateway settlements, cash recon, multi-currency).
- Stack: Go. Scale: 1M–50M records/day. Deployment: multi-tenant SaaS.
- Must-win differentiators vs ReconArt (priority order): matching engine flexibility/speed → exception/case management workflow → real-time/streaming recon → open API & extensibility.

## Parts

| File | Covers |
|---|---|
| [00-overview-and-architecture.md](00-overview-and-architecture.md) | Executive summary, modular-monolith vs microservices decision, service topology diagram, tech stack + justification |
| [01-multi-tenancy.md](01-multi-tenancy.md) | Isolation tiers (shared RLS / isolated schema / dedicated), tenant routing, config storage, quotas/noisy-neighbor mitigation |
| [02-data-ingestion.md](02-data-ingestion.md) | Connector architecture (SFTP/MT940/BAI2/ISO20022/API/Kafka/webhook), schema mapping DSL, validation, idempotency/dedup, batch+streaming convergence |
| [03-canonical-data-model.md](03-canonical-data-model.md) | Core entities (Tenant, Account, Statement, Transaction, MatchRule, MatchResult, Break/Case, AuditEvent), relationships, multi-currency normalization |
| [04-matching-engine.md](04-matching-engine.md) | **Core differentiator.** Rule DSL, batch/streaming matching algorithms at scale, fuzzy matching techniques, confidence scoring, performance targets |
| [05-case-management.md](05-case-management.md) | Break lifecycle (Temporal-orchestrated), auto-assignment, SLA/escalation, maker-checker approvals, audit trail, root-cause taxonomy |
| [06-streaming-architecture.md](06-streaming-architecture.md) | Kafka topic design, Protobuf event schema, exactly-once model, backpressure, hybrid batch/streaming reconciliation model |
| [07-api-extensibility.md](07-api-extensibility.md) | REST/gRPC/Connect-RPC design, webhook system, plugin/Connector SDK (WASM-sandboxed), GraphQL reporting gateway |
| [08-storage-architecture.md](08-storage-architecture.md) | Postgres+Citus (OLTP), ClickHouse (analytics), CDC sync, 7+ year retention/archival strategy |
| [09-security-compliance.md](09-security-compliance.md) | Immutable hash-chained audit log, SOC2/PCI-DSS, encryption, RBAC/ABAC (OPA), data residency, WORM storage |
| [10-observability-reliability.md](10-observability-reliability.md) | SLOs, metrics/tracing, idempotent replay/reprocessing, disaster recovery, deployment strategy (blue-green/canary) |
| [11-scalability-roadmap.md](11-scalability-roadmap.md) | Scaling 1M→50M+ records/day, phased roadmap (MVP → V1 → V2) |
| [12-competitive-differentiation.md](12-competitive-differentiation.md) | ReconArt weakness → Jengine answer mapping table |
| [13-implementation-notes.md](13-implementation-notes.md) | First files to scaffold for Phase 0, verification/next steps before build starts |
| [14-dashboard-frontend.md](14-dashboard-frontend.md) | Frontend stack (Next.js/TanStack/shadcn), key screens (case queue, match review, rule builder, tenant admin), SSE real-time updates, phased build order (built last, planned now) |
| [15-end-to-end-flows.md](15-end-to-end-flows.md) | **Read this to understand the whole system.** Glossary + 5 concrete step-by-step flows tying every module/table/topic together: batch ingestion→match→case, streaming+hybrid recon, rule authoring/approval, tenant onboarding, failure/redrive |
| [16-development-workflow.md](16-development-workflow.md) | Repo directory layout, local dev stack (docker-compose), config/secrets convention, dependency wiring, testing strategy, CI pipeline stages |

## Reading Order
- New to the project? Read **00 → 15 → 04 → 05** first (architecture, then the full flow walkthrough, then the two most important differentiators in depth).
- Implementing a specific area? Jump directly to its file — each is self-contained enough to not require loading the others into context.
- Before writing any code: read **15-end-to-end-flows.md** and **16-development-workflow.md**.
- Building the UI? Read **14-dashboard-frontend.md** — but note it's scoped to build *after* the backend/API stabilizes (see its §14.4 for phase placement).
