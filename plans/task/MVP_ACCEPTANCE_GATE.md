# MVP Acceptance Gate

This is not a build task — it's the checkpoint referenced by [`OPERATING_INSTRUCTIONS.md`](OPERATING_INSTRUCTIONS.md) §6: *"do not start `core/` 18+ or `frontend/` 08+ until the MVP set is working end-to-end and verified."*

Every task 01–17 (core) and 01–07 (frontend) has its own Definition of Done, verified in isolation (unit tests, integration tests against its own scope). None of them, individually or summed, prove that **the whole system wired together actually reconciles a transaction** — that gap is exactly how a build can look "done" task-by-task while the real product doesn't work. This file closes that gap.

Do not start any V1 task until every check below passes for real, against the real docker-compose dev stack (`plans/task/core/02`), not mocked.

## Why this exists (concrete failure mode it prevents)

Task 07 might correctly parse an MT940 file in isolation. Task 12 might correctly run the batch matcher against a synthetic in-memory dataset. Task 15's API might correctly serve whatever's in the DB. Each passes its own tests. But if task 07's connector never actually emits onto the topic task 12's worker reads from — because of a topic-name typo, a schema-shape mismatch, or a wiring gap between two `cmd/*` binaries that were never run *together* — the product is broken despite 24 green task-level DoDs. Only running the real pipeline end-to-end catches that class of bug.

## Scenario 1 — Auto-match (happy path)

1. Start the full local stack (`make dev-up`), run migrations (`make migrate`), seed one tenant (`make seed`).
2. Drop a fixture MT940 statement (bank side) and a fixture GL export (CSV, other side) containing two transactions that should auto-match under a simple exact-match rule (same amount, same date, matching reference).
3. Trigger ingestion for both (via the real connectors, task 06/07 — not by inserting rows directly into Postgres).
4. Trigger the batch matcher (task 12) for the affected account pair.
5. **Verify via the real REST API (task 15)**, not a DB query: `GET` the transactions and confirm both show `status=MATCHED`, and a `MatchResult(status=AUTO_MATCHED)` exists linking them.
6. **Verify via the frontend** (task 07, Overview Dashboard): the match-rate tile reflects this match (may require the Postgres-materialized-view fallback per MVP scope, per `plans/docs/11-scalability-roadmap.md` §12.2 Phase 0 — ClickHouse is V1).

## Scenario 2 — Suggested match → analyst confirms

1. Ingest a bank transaction and a GL transaction whose amount/reference are close but not exact (e.g. reference has a typo) — score should land in `[suggest, auto_match)`.
2. Run the batch matcher. Confirm a `MatchResult(status=SUGGESTED)` exists.
3. **Verify via the frontend** (task 05, Match Review screen): the suggested match appears with its confidence-score breakdown.
4. Confirm it via the real API call the UI would make (task 15's `ConfirmMatch`). Verify both transactions flip to `MATCHED` and the `MatchResult` flips to `CONFIRMED`.

## Scenario 3 — No match → break → case lifecycle → resolution

1. Ingest one bank transaction with no plausible counterpart at all.
2. Run the batch matcher. Confirm it does **not** produce any `MatchResult`, and instead a `Break(status=OPEN)` is created.
3. Confirm auto-assignment ran (task 13): `assigned_to` and `sla_due_at` are set, not null.
4. **Verify via the frontend** (task 03/04, Case Queue + Case Detail): the break appears in the queue, opening it shows the linked transaction.
5. Add a comment and resolve the case through the real API (task 15's `AddComment` / `TransitionBreak`). Confirm `CaseAuditEvent` and the global `AuditEvent` (task 14) both recorded the transition, and the hash chain on the latest `AuditEvent` verifies.

## Scenario 4 — Bulk action (the gap the cross-task audit caught)

1. Create at least 3 open breaks for the same tenant.
2. Use the frontend's bulk-select (task 03) to assign all three at once via the real `BulkAssignBreaks` endpoint (task 15/13).
3. Confirm the API response reports a correct per-ID result (`BulkResult`), and confirm exactly **one** audit event was written for the batch operation, not three.

## Gate result

- All four scenarios pass → MVP is accepted. Record this in a commit (e.g. `chore: MVP acceptance gate passed`) — this commit *is* the record; don't add a marker file or checklist for it.
- Anything fails → it is a real defect in the wiring between tasks, not a "this task's own tests are wrong" situation. Fix the actual integration bug, re-run the failing scenario (not just the whole suite blindly), and only then continue.
- If a scenario can't even be attempted because a piece genuinely doesn't exist yet (e.g. no seed data tooling), that's a gap in an earlier task's Definition of Done that was marked done incorrectly — go re-open that task per `OPERATING_INSTRUCTIONS.md` §5, don't work around it here.
