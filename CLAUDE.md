# Jengine — Reconciliation Engine

Multi-tenant financial reconciliation platform (Go backend, Next.js frontend), designed to compete with ReconArt. Greenfield build, no legacy code.

## Start here, always

- **Design**: [`plans/docs/README.md`](plans/docs/README.md) — 17 files covering architecture, multi-tenancy, ingestion, canonical data model, matching engine, case workflow, streaming, API, storage, security, observability, roadmap, dashboard, end-to-end flows, dev workflow.
- **Build tasks**: [`task/README.md`](task/README.md) — the numbered build sequence (`task/core/01`–`26`, `task/frontend/01`–`11`).
- **Process rules — read before doing any task work**: [`task/OPERATING_INSTRUCTIONS.md`](task/OPERATING_INSTRUCTIONS.md). Covers: verifying prerequisites are *actually* done (not just lower-numbered), what to do on cross-task conflicts, the `QA_REPORT.md` convention, and that Definition-of-Done must be verified by running tests, never asserted.

## Non-negotiable conventions

- **Build order**: `core/` 01–17 and `frontend/` 01–07 are MVP, build in that order. Do not start `core/` 18+ or `frontend/` 08+ (V1) until MVP passes [`task/MVP_ACCEPTANCE_GATE.md`](task/MVP_ACCEPTANCE_GATE.md).
- **One commit per task** (or per fix), referencing the task file in the commit message. Git history is one of the two sources of truth for "is this done" — the other is the test suite. Task files themselves never get a "✅ Done" marker.
- **QA_REPORT.md** (repo root, created on first use): holds *only currently open* issues. Fix + re-verify → delete the entry, don't check it off. Never create a second QA file.
- **Cross-task conflicts**: small unambiguous gaps (e.g. a missing enum value) — fix directly, note it. Real design-level contradictions — stop, log in `QA_REPORT.md`, surface for a human decision. Don't guess on anything a schema/contract/security choice depends on.
- Module path, directory skeleton, and tooling (Makefile/lint/CI) are established by `task/core/01` — once that task is done, its output is the actual source of truth for structure, not this file.

## Stack (see `plans/docs/00-overview-and-architecture.md` §1.3 for full rationale)

Go 1.23+ · Connect-RPC (gRPC+REST+gRPC-Web from one proto) · Redpanda (Kafka-API) · PostgreSQL+Citus · ClickHouse · Temporal · Redis · Next.js/TypeScript frontend.
