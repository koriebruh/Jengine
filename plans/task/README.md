# Jengine — Implementation Task Set

This folder is the **build sequence** for Jengine, derived from the design in [`../docs/`](../docs/README.md). Each file in `core/` and `frontend/` is one implementation task, numbered in the order it should be built. An AI coding agent should be able to open a single task file and implement it correctly without re-reading the entire design doc set — task files reference the relevant design sections instead of repeating them.

**Read [`OPERATING_INSTRUCTIONS.md`](OPERATING_INSTRUCTIONS.md) before starting any task.** It's the process contract — how to verify prerequisites are really done, how to handle cross-task conflicts, how Definition of Done gets verified (not asserted), and the `QA_REPORT.md` convention. This README describes *what's in the folder*; that file describes *how to move through it safely*.

## Why two folders

- **`core/`** — everything with no UI: infra, schema, domain models, ingestion, matching engine, case workflow, API layer, security, observability. This is built first because the frontend is a client of this layer's API contract (see [`../docs/14-dashboard-frontend.md`](../docs/14-dashboard-frontend.md), which explicitly says the frontend is built last).
- **`frontend/`** — the Next.js dashboard. Each frontend task depends on specific core tasks being done (stated in its Prerequisites section) because it consumes their API/data contracts.

## Build order

Tasks are numbered for sequential execution within each folder. `core/` and `frontend/` are not fully sequential relative to each other — frontend task 01 (bootstrap) can start once `core/06` (ingestion framework) and `core/15` (REST API MVP) exist; each frontend task states its actual core prerequisite explicitly rather than assuming "all of core is done first."

Tasks numbered 18+ in `core/` and 08+ in `frontend/` are **V1-phase** work (see [`../docs/11-scalability-roadmap.md`](../docs/11-scalability-roadmap.md) §12.2) — do not start these until the MVP tasks (core 01–17, frontend 01–07) pass [`MVP_ACCEPTANCE_GATE.md`](MVP_ACCEPTANCE_GATE.md), which proves the tasks work wired together end-to-end, not just individually. `core/26-v2-backlog-notes.md` is a short pointer file, not a buildable task — it exists so V2 ideas aren't lost, not so they get built early.

## Task file format

Every task file follows the same structure so an agent always knows where to look for a given kind of information:

- **Goal** — what this task builds and why it exists in the system.
- **Prerequisites** — which earlier task(s) must be done first.
- **Scope / Deliverables** — concrete files/modules to create or modify.
- **Design Reference** — pointers into `plans/docs/` for the *why* behind decisions. Task files do not repeat this content — read the reference if the "why" isn't obvious.
- **Implementation Notes** — logic-level detail: struct fields, function signatures, algorithm steps, edge cases. Detailed enough that the agent isn't guessing at architecture, but still a spec, not literal code.
- **Non-Goals / Guardrails** — what NOT to build in this task (deferred to a later one). This is the main defense against scope creep and an agent wandering into unrelated work.
- **Definition of Done** — see below.
- **Common Pitfalls** — specific, concrete mistakes an agent could make here that would contradict the design.

## Definition of Done — no checklist clutter

A task is done when its stated tests pass (unit, integration, or golden-dataset per [`../docs/16-development-workflow.md`](../docs/16-development-workflow.md) §16.4) and, where relevant, a manual verification step succeeds. **Tests are the completion record — not a markdown checklist.**

If manual/exploratory QA turns up issues while working a task:
- Track them in a single root-level `QA_REPORT.md` (create it if it doesn't exist) — it holds only *currently open* issues, nothing else.
- When an issue is fixed and re-verified, **delete its entry**, don't check it off. An empty `QA_REPORT.md` means clean state.
- Never create a second QA file (`qa-report-2.md`, `qa-final.md`, etc.) — always edit the same one in place.
- Day-to-day "what am I doing right now" tracking belongs in the session's task-tracking tool, not in a committed file — in-progress checklists shouldn't become permanent repo artifacts.
- Task files in this folder themselves are never edited to add a "✅ Done" marker — completion is what the test suite says, and what git history says (one commit per task/fix). If a task's scope turns out to be wrong once you're implementing it, fix the task file's content, don't append a status marker to it.

## Core task list

| # | Task | Phase |
|---|---|---|
| 01 | Repo bootstrap and tooling | MVP |
| 02 | Local dev infrastructure (docker-compose) | MVP |
| 03 | Database schema and migrations | MVP |
| 04 | Tenancy context and routing | MVP |
| 05 | Canonical domain models and repositories | MVP |
| 06 | Ingestion connector framework | MVP |
| 07 | Ingestion MVP connectors (CSV/SFTP/MT940) | MVP |
| 08 | Field mapping and normalization | MVP |
| 09 | Idempotency and validation | MVP |
| 10 | Matching engine core library | MVP |
| 11 | Matching rule DSL | MVP |
| 12 | Matching batch worker | MVP |
| 13 | Case/break lifecycle (MVP state machine) | MVP |
| 14 | Audit logging | MVP |
| 15 | REST API layer (MVP) | MVP |
| 16 | Observability baseline | MVP |
| 17 | Testing and CI pipeline | MVP |
| 18 | Streaming ingestion (Kafka/Redpanda) | V1 |
| 19 | Streaming matching worker | V1 |
| 20 | Temporal workflow migration for case lifecycle | V1 |
| 21 | Webhook system | V1 |
| 22 | ClickHouse analytics pipeline | V1 |
| 23 | Security hardening (RBAC/ABAC via OPA) | V1 |
| 24 | Full multi-tenancy isolation tiers | V1 |
| 25 | Connector SDK and extensibility (WASM) | V1 |
| 26 | V2 backlog notes (pointer only, not a task) | V2 |

## Frontend task list

| # | Task | Phase |
|---|---|---|
| 01 | Frontend bootstrap (Next.js) | MVP |
| 02 | API client codegen and auth | MVP |
| 03 | Case queue screen | MVP |
| 04 | Case detail screen | MVP |
| 05 | Match review screen | MVP |
| 06 | Connector monitor screen | MVP |
| 07 | Overview dashboard screen | MVP |
| 08 | Rule builder screen (with backtesting sandbox) | V1 |
| 09 | Tenant admin screen | V1 |
| 10 | Real-time SSE integration | V1 |
| 11 | Audit viewer screen | V1 |
