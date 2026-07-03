# Task 13: Case/Break Lifecycle (MVP State Machine)

## Goal
Build `internal/cases`, the MVP implementation of the Break/Case lifecycle: a plain Go state machine backed by Postgres, with manual assignment and a comment trail. `plans/docs/05-case-management.md` describes the full target design (every Break backed by a durable Temporal workflow instance, giving durable SLA timers, Signal-based human-in-the-loop, auto-assignment, maker-checker `ApprovalWorkflow` child workflows). `plans/docs/11-scalability-roadmap.md` §12.2 Phase 0 is explicit that MVP does not build Temporal — "a simple state machine + Postgres, no Temporal yet... upgrade path planned, not blocking." This task builds that simple version, but behind an interface boundary designed so that task 20 (Temporal workflow migration for case lifecycle, V1) can swap the implementation without rewriting any caller — the batch worker (task 12), the API layer (task 15), and anything else that opens/transitions/comments on a break depends only on the interface defined here, never on the Postgres-specific internals.

## Prerequisites
- Task 03 (database schema and migrations) — `Break`/`Case`, `CaseComment`, `CaseAuditEvent` tables per `plans/docs/03-canonical-data-model.md` §4.1.
- Task 05 (canonical domain models and repositories) — base repository patterns this task's Postgres-backed implementation follows.
- Task 10 (matching engine core library) — this task provides the concrete implementation of `core.BreakSink`, the interface task 12 calls to open breaks from unmatched residue.
- Task 12 (matching batch worker) is the primary caller of `OpenBreak` at MVP — task 12 and 13 have a two-way dependency (12 depends on 13's `BreakSink` implementation to run end-to-end; 13's `LifecycleService` interface is defined independently in `internal/cases` and doesn't require task 12 to exist first). Build the interface and Postgres implementation here regardless of task 12's build order.

## Scope / Deliverables
- `internal/cases/lifecycle.go` — the `LifecycleService` interface (the clean boundary task 20 will implement against later) and its MVP Postgres-backed implementation.
- `internal/cases/breaksink.go` — `core.BreakSink` implementation adapting `OpenBreakParams` into a `Break` row + initial state.
- `internal/cases/state.go` — `BreakStatus` enum and the allowed-transition table.
- `internal/cases/comments.go` — `CaseComment` append-only writer/reader.
- `internal/cases/rootcause.go` — seeded default root-cause taxonomy (`plans/docs/05-case-management.md` §6.6), tenant-extensible lookup.
- `internal/cases/repository.go` — Postgres repository for `Break`/`CaseComment`/`CaseAuditEvent` (or reuse task 05's generic repository patterns if applicable — don't duplicate infrastructure task 05 already built).

## Design Reference
- `plans/docs/05-case-management.md` §6.1 — the lifecycle diagram (`OPEN → ASSIGNED → IN_PROGRESS → PENDING_APPROVAL → RESOLVED`, plus `ESCALATED` and `WRITTEN_OFF` branches, `REOPENED` on new evidence). This task implements the state transitions as plain application logic, not Temporal Activities/Signals.
- `plans/docs/05-case-management.md` §6.4 — maker-checker approval semantics (`maker != checker`) — MVP implements this as a synchronous validation check at transition time (reject a `DecideApproval` call where approver == the original requester), **not** as a durable `ApprovalWorkflow` child workflow with automatic reminders/multi-level chains — that is explicitly task 20/V1 work.
- `plans/docs/05-case-management.md` §6.5 — the two-tier audit model: `CaseComment`/`CaseAuditEvent` (UX-optimized case-level trail) vs. the global hash-chained `AuditEvent` (task 14, compliance-grade). This task writes to both — every state transition and comment produces a `CaseAuditEvent` locally *and* calls task 14's writer for the global chain.
- `plans/docs/03-canonical-data-model.md` §4.1 `Break`/`CaseComment`/`CaseAuditEvent` entities — note `Break.temporal_workflow_id` exists in the schema for the eventual Temporal-backed version; this task leaves it `NULL` for every row it creates (task 20 populates it when migrating a tenant/case to Temporal orchestration).
- `plans/docs/11-scalability-roadmap.md` §12.2 Phase 0 — "manual assignment only, comment trail" — no auto-assignment routing engine, no SLA timers/escalation automation in this task.

## Implementation Notes

### State enum and transitions
```go
type BreakStatus string

const (
    BreakOpen             BreakStatus = "OPEN"
    BreakAssigned         BreakStatus = "ASSIGNED"
    BreakInProgress       BreakStatus = "IN_PROGRESS"
    BreakPendingApproval  BreakStatus = "PENDING_APPROVAL"
    BreakResolved         BreakStatus = "RESOLVED"
    BreakWrittenOff       BreakStatus = "WRITTEN_OFF"
    BreakEscalated        BreakStatus = "ESCALATED"
    BreakReopened         BreakStatus = "REOPENED"
)
```
Note: `plans/docs/03-canonical-data-model.md` §4.1 lists `status (OPEN|ASSIGNED|IN_PROGRESS|PENDING_APPROVAL|RESOLVED|WRITTEN_OFF|ESCALATED)` without `REOPENED`, while §6.1's lifecycle diagram shows a `REOPENED` transition on new evidence. This has already been reconciled: task 03's `cases.status` `CHECK` constraint includes `REOPENED` directly (see task 03's schema, updated during cross-task reconciliation) — no supplementary migration needed here, just use the value.

`allowedTransitions map[BreakStatus][]BreakStatus` encodes the diagram directly: `OPEN → {ASSIGNED}`, `ASSIGNED → {IN_PROGRESS, ESCALATED}`, `IN_PROGRESS → {PENDING_APPROVAL, RESOLVED, ESCALATED}`, `PENDING_APPROVAL → {RESOLVED, ASSIGNED}` (rejection returns to work), `ESCALATED → {ASSIGNED}` (re-assigned, SLA clock adjusted — SLA clock adjustment itself is a no-op at MVP since there's no SLA timer yet, but the state transition still happens), any non-terminal state `→ {WRITTEN_OFF}` (requires approval — enforced by requiring the transition go through `PENDING_APPROVAL` first), `RESOLVED/WRITTEN_OFF → {REOPENED}`, `REOPENED → {ASSIGNED}`.

### LifecycleService interface (the swap boundary for task 20)
```go
type Actor struct {
    UserID string
    Role   string
}

type LifecycleService interface {
    OpenBreak(ctx context.Context, params OpenBreakParams) (Break, error)
    Assign(ctx context.Context, breakID uuid.UUID, assignee string, actor Actor) error
    Transition(ctx context.Context, breakID uuid.UUID, to BreakStatus, actor Actor, comment string) error
    AddComment(ctx context.Context, breakID uuid.UUID, actor Actor, body string) (CaseComment, error)
    RequestApproval(ctx context.Context, breakID uuid.UUID, actor Actor) error
    DecideApproval(ctx context.Context, breakID uuid.UUID, approver Actor, approve bool, comment string) error
    TagRootCause(ctx context.Context, breakID uuid.UUID, category string, actor Actor) error

    // Bulk variants — required per plans/docs/05-case-management.md §6.2:
    // "Bulk actions: multi-select assign/comment/resolve, single audit event
    // referencing batch-op id + affected case ids." Frontend task 03 (Case
    // Queue)'s row-selection bulk-action toolbar depends on these existing;
    // this was missed in the first pass of this task and is added here as a
    // cross-task audit finding — do not treat it as optional. Each writes
    // exactly ONE CaseAuditEvent/AuditEvent per batch operation (carrying a
    // generated batch_op_id and the full list of affected break IDs), not one
    // audit event per break — that is the specific point of "single audit
    // event referencing batch-op id."
    BulkAssign(ctx context.Context, breakIDs []uuid.UUID, assignee string, actor Actor) (BulkResult, error)
    BulkTransition(ctx context.Context, breakIDs []uuid.UUID, to BreakStatus, actor Actor, comment string) (BulkResult, error)
    BulkAddComment(ctx context.Context, breakIDs []uuid.UUID, actor Actor, body string) (BulkResult, error)
}

// BulkResult reports a per-ID outcome, since a bulk call over a heterogeneous
// selection (mixed current statuses) can legitimately partially fail — e.g.
// bulk-resolving 10 selected breaks where 2 are already RESOLVED must not
// silently succeed-as-a-whole or fail-as-a-whole. The caller (task 15's API
// handler, ultimately the frontend bulk-action bar) needs to know exactly
// which IDs succeeded and why any failed.
type BulkResult struct {
    BatchOpID uuid.UUID
    Succeeded []uuid.UUID
    Failed    map[uuid.UUID]string // break ID -> error reason (e.g. "invalid transition from RESOLVED")
}
```
Every method call above: (1) validates the transition against `allowedTransitions`, (2) applies it in a Postgres transaction alongside inserting the corresponding `CaseAuditEvent` row, (3) calls out to task 14's `audit.Writer` to append the same event to the global hash-chained log, (4) returns. This is the interface task 15 (API layer) and task 12 (via `BreakSink`) depend on. **Task 20 replaces the Postgres-backed implementation with a Temporal-orchestrated one implementing this exact same interface** — callers must not need to change. Keep this interface's method signatures free of any Postgres-specific types (no `*sql.Tx` parameters, no repository types) to make that swap actually clean.

Bulk methods still validate each break's transition individually against `allowedTransitions` (a bulk call is not a license to skip per-item validation) and still enforce the `WRITTEN_OFF`-requires-`PENDING_APPROVAL` gate per-item — only the audit-event granularity changes (one event for the whole batch), not the transition-safety rules.

`DecideApproval` enforces maker != checker: store the requesting actor's `UserID` when `RequestApproval` is called (e.g. on the `Break` row or a small approval-request record), and reject `DecideApproval` if `approver.UserID` matches it.

### BreakSink adapter
```go
func (s *postgresLifecycleService) OpenBreak(ctx context.Context, p core.OpenBreakParams) error {
    _, err := s.LifecycleService.OpenBreak(ctx, OpenBreakParams{
        TenantID: p.TenantID, AccountID: p.AccountID,
        RelatedTransactionIDs: p.TransactionIDs, BreakType: p.BreakType,
        AmountAtRisk: p.AmountAtRisk, Currency: p.Currency,
    })
    return err
}
```
This is the piece `cmd/matching-batch/main.go` (task 12) constructs and passes in as `core.BreakSink` — the only place both `internal/matching/core` and `internal/cases` types meet.

### Root-cause taxonomy
Seed the defaults from §6.6 (`Timing Difference`, `Data Entry Error`, `Duplicate Transaction`, `FX Rate Variance`, `Missing Counterparty Statement`, `System Interface Failure`, `Fraud/Investigation`, `Fee/Charge Discrepancy`, `Unauthorized Transaction`) as a simple lookup (table or seed migration data), tenant-extensible (a tenant can add custom categories — a lightweight `tenant_root_cause_category` table or JSONB list on tenant settings; keep this simple, it's not a workflow-critical piece at MVP).

## Non-Goals / Guardrails
- No Temporal — no workflow definitions, no Activities, no Signals, no `temporalio` SDK dependency in this task. `Break.temporal_workflow_id` stays `NULL`. That's task 20.
- No durable SLA timers, no auto-escalation on breach, no `sla.breached` webhook — SLA *tracking fields* (`sla_due_at`) may be computed and stored if trivial, but no timer/automation fires transitions; escalation is a manual analyst action at MVP, not a system one.
- No auto-assignment routing engine (round-robin/load-balanced/skill-based per §6.2) — `Assign` is a direct, explicit, manually-specified assignee call only.
- No multi-level approval chains, no automatic approval reminders — `DecideApproval` is a single maker/single checker check, nothing more.
- No OPA/ABAC authorization enforcement on who can call which method (task 23, V1) — `Actor` is captured for audit purposes; RBAC-role gating of these calls happens at the API layer (task 15) at a coarse level if at all, not deeply enforced here.
- Do not import `internal/matching/core` types directly into method signatures beyond what's needed to satisfy `core.BreakSink` in the adapter file — keep the main `LifecycleService` interface free of matching-package types.

## Definition of Done
- Unit tests for the transition table: every allowed transition succeeds, every disallowed one is rejected with a clear error, `REOPENED` handling works.
- Unit test for maker != checker rejection in `DecideApproval`.
- Integration test (testcontainers-go, real Postgres): `OpenBreak` → `Assign` → `Transition(IN_PROGRESS)` → `AddComment` → `RequestApproval` → `DecideApproval(approve=true)` → `Transition(RESOLVED)`, asserting `Break` row state, `CaseComment`/`CaseAuditEvent` rows, and (via a test double or real task-14 writer) that a global `AuditEvent` was appended for each transition.
- Integration test for the `core.BreakSink` adapter: calling `OpenBreak` with `core.OpenBreakParams` produces the correct `Break` row.
- Integration test for bulk operations: `BulkAssign`/`BulkTransition`/`BulkAddComment` over a mixed-status selection produces exactly one `CaseAuditEvent`/`AuditEvent` per batch op (not per break), correctly reports partial failure in `BulkResult.Failed` for IDs whose transition was invalid, and does not roll back the IDs that succeeded because one ID in the batch failed.
- `go test -race ./internal/cases/...` passes.
- Manual verification: exercise the full lifecycle against the local dev stack (task 02) via direct Go calls or a small test harness (the HTTP surface is task 15's job, not required here) and confirm state/audit rows look correct in Postgres.
- Completion is tests passing; exploratory QA issues go in root-level `QA_REPORT.md`, open items only, deleted when fixed.

## Common Pitfalls
- Designing `LifecycleService`'s interface around Postgres-specific concepts (passing transactions, repository structs, or SQL-shaped types through the interface) — this defeats the entire purpose of building this task as a "clean swap boundary" for task 20; keep the interface Postgres-agnostic.
- Implementing durable SLA timers via a naive cron/poll loop "since Temporal isn't available yet" — this is exactly the fragile pattern `plans/docs/00-overview-and-architecture.md` §1.3 says Temporal was chosen specifically to avoid; the MVP answer is simply not to build automated SLA enforcement yet, not to build a worse version of it.
- Treating a `SUGGESTED` match rejection (an analyst explicitly rejecting a suggested match in the Match Review Queue, task 15/frontend) as needing separate break-creation logic from `BreakSink.OpenBreak` — both paths (unmatched residue from task 12, and analyst-rejected suggestions from task 15) should funnel through the same `OpenBreak` call, not diverge into parallel break-creation code.
- Only writing to `CaseAuditEvent` and forgetting the global `AuditEvent` call (task 14) — per §6.5 these are deliberately two separate writes with two separate purposes (UX trail vs. compliance-grade hash chain); skipping the global one silently breaks the audit chain's completeness.
- Hardcoding `WRITTEN_OFF` as reachable directly from any state without requiring `PENDING_APPROVAL` first — §6.4 is explicit that write-offs require approval; don't let `Transition` skip that gate for convenience.
