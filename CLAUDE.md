# Jengine — Reconciliation Engine

Multi-tenant financial reconciliation platform (Go backend, Next.js frontend), designed to compete with ReconArt. Greenfield build, no legacy code.

## Start here, always

- **Design**: [`plans/docs/README.md`](plans/docs/README.md) — 17 files covering architecture, multi-tenancy, ingestion, canonical data model, matching engine, case workflow, streaming, API, storage, security, observability, roadmap, dashboard, end-to-end flows, dev workflow.
- **Build tasks**: [`plans/task/README.md`](plans/task/README.md) — the numbered build sequence (`plans/task/core/01`–`26`, `plans/task/frontend/01`–`11`).
- **Process rules — read before doing any task work**: [`plans/task/OPERATING_INSTRUCTIONS.md`](plans/task/OPERATING_INSTRUCTIONS.md). Covers: verifying prerequisites are *actually* done (not just lower-numbered), what to do on cross-task conflicts, the `QA_REPORT.md` convention, and that Definition-of-Done must be verified by running tests, never asserted.

## Non-negotiable conventions

- **Build order**: `core/` 01–17 and `frontend/` 01–07 are MVP, build in that order. Do not start `core/` 18+ or `frontend/` 08+ (V1) until MVP passes [`plans/task/MVP_ACCEPTANCE_GATE.md`](plans/task/MVP_ACCEPTANCE_GATE.md).
- **One commit per task** (or per fix), referencing the task file in the commit message. Git history is one of the two sources of truth for "is this done" — the other is the test suite. Task files themselves never get a "✅ Done" marker.
- **QA_REPORT.md** (repo root, created on first use): holds *only currently open* issues. Fix + re-verify → delete the entry, don't check it off. Never create a second QA file.
- **Cross-task conflicts**: small unambiguous gaps (e.g. a missing enum value) — fix directly, note it. Real design-level contradictions — stop, log in `QA_REPORT.md`, surface for a human decision. Don't guess on anything a schema/contract/security choice depends on.
- Module path, directory skeleton, and tooling (Makefile/lint/CI) are established by `plans/task/core/01` — once that task is done, its output is the actual source of truth for structure, not this file.

## Stack (see `plans/docs/00-overview-and-architecture.md` §1.3 for full rationale)

Go 1.23+ · Connect-RPC (gRPC+REST+gRPC-Web from one proto) · Redpanda (Kafka-API) · PostgreSQL+Citus · ClickHouse · Temporal · Redis · Next.js/TypeScript frontend.

## MCP servers (`.mcp.json`)

Project-scoped, loaded automatically for anyone opening this repo. Restart the session / run `/mcp` after editing this file.

| Server | Purpose | Notes |
|---|---|---|
| `context7` | Up-to-date library/framework docs — use for any library/CLI/API question instead of relying on training data. | Works immediately, no local dependency. Optional `CONTEXT7_API_KEY` env var raises the rate limit if needed later. |
| `shadcn` | shadcn/ui component listing/install/scaffolding tools. | Most useful once `plans/task/frontend/01` (Next.js bootstrap) exists. |
| `postgres` | Query/inspect the local dev Postgres directly. | Connection string matches `plans/task/core/02`'s planned dev defaults (`jengine`/`jengine_dev`@`localhost:5432`/`jengine`). Won't connect until `make dev-up` (task 02) is actually built and running — that's expected, not broken. |
| `redis` | Inspect/query the local dev Redis directly (candidate-index cache, rate-limit counters, idempotency bloom filter, rolling-match-window state). | Runs as a container (official `mcp/redis` image via `docker run`), not a native install — matches the project's "infra runs in containers" preference. Uses `host.docker.internal` to reach the docker-compose Redis service from task 02 on `localhost:6379`. |
| `mcp-clickhouse` | Query/inspect ClickHouse once it exists. | ClickHouse itself isn't part of the MVP stack — it arrives in `plans/task/core/22` (V1). Pre-staged, not usable until then. No Docker image exists (build-from-source only), so it runs via `uvx` at an absolute path (`/home/jamalkya/.local/bin/uvx`) — a deliberate exception to the container-first preference. |
| `memory` | Generic knowledge-graph memory server (`@modelcontextprotocol/server-memory`). | Separate from Claude Code's own project-memory system — this is an MCP tool an agent can call explicitly to store/recall structured facts mid-session. |

Playwright is **not** added here — it's already available globally as an installed plugin in this environment. Add it project-scoped only if a fresh environment (CI, another machine) needs it and doesn't have that plugin.

**Deliberately not added:** `github` MCP (excluded per explicit decision). `kafka` and `mcp-server-docker` MCP servers were added then explicitly removed again — not needed right now; re-evaluate if/when V1 streaming tasks (`plans/task/core/18-20`) actually need Kafka/Redpanda inspection tooling. Temporal MCP and MinIO/S3 MCP were checked and skipped outright — best options found had 1★ and 4★ respectively, not worth adding at any point without re-checking for a matured option later.
