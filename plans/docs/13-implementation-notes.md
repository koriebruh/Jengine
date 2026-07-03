> Part of the Jengine Reconciliation Engine design doc set. Index: [README.md](README.md) · Prev: [12-competitive-differentiation.md](12-competitive-differentiation.md)

# 13 — Implementation Notes

## First Files to Scaffold (Phase 0 kickoff)

Since this is a greenfield Go project with no existing code, these are the first files/modules to scaffold when implementation begins:

- `proto/jengine/v1/transaction.proto` — canonical event/API schema definitions (source of truth for both gRPC APIs and Kafka event contracts)
- `internal/matching/core/engine.go` — shared blocking-key + scoring library used by both batch workers and streaming consumers (the single most important correctness-critical module, see [04-matching-engine.md](04-matching-engine.md))
- `internal/matching/rules/dsl.go` — rule DSL parser/compiler (JSON/YAML → executable rule AST)
- `internal/tenancy/context.go` — `TenantContext` propagation and shard/isolation-tier routing (foundational for every other module's safety, see [01-multi-tenancy.md](01-multi-tenancy.md))
- `internal/ingestion/connector.go` — `SourceConnector` interface and connector registry (defines the extensibility contract from day one, see [02-data-ingestion.md](02-data-ingestion.md))
- `migrations/0001_init_schema.sql` — initial Postgres/Citus schema (distribution columns, RLS policies) establishing the multi-tenant data model foundation (see [03-canonical-data-model.md](03-canonical-data-model.md))

## Verification / Next Step

This is a design document set — no code to run yet. To validate before Phase 0 build starts:

1. Review this doc set against actual design-partner requirements (if any specific bank/fintech is lined up as first customer, cross-check their file formats/volumes against the connector list in [02-data-ingestion.md](02-data-ingestion.md) §3.1 and scale assumptions in [11-scalability-roadmap.md](11-scalability-roadmap.md) §12.1).
2. Confirm Go stack choice covers team's existing skillset for Temporal, Citus, ClickHouse operational learning curve — these are the three components with the steepest ops learning curve if the team hasn't run them before.
3. Once approved, start Phase 0 MVP scope only (see [11-scalability-roadmap.md](11-scalability-roadmap.md) §12.2) — do not build V1/V2 components (Temporal, Kafka streaming, ClickHouse) until MVP validates core matching+workflow value with design partners.

---
Next: [14-dashboard-frontend.md](14-dashboard-frontend.md)
