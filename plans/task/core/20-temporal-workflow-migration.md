# Task 20: Temporal Workflow Migration for Case Lifecycle

## Goal
Replace MVP's simple Postgres state-machine case lifecycle (core task 13) with the full Temporal-orchestrated design: durable SLA timers that survive restarts, human-in-the-loop actions via Signals, a maker-checker `ApprovalWorkflow` child workflow, and auto-assignment implemented as a Temporal Activity. This is one of Jengine's four must-win differentiators (exception/case management workflow) and the design doc is explicit that this is a planned upgrade, not a redesign done under time pressure — task 13 was deliberately built with this migration in mind.

## Prerequisites
- Core task 13 (MVP simple state machine — its public interface is what callers depend on and must be preserved).
- Core task 03 (schema — `Break.temporal_workflow_id` column already exists per the canonical data model; this task starts populating and using it, not creating it).
- Temporal dev server is already part of `deploy/docker-compose.dev.yml` from core task 02 (provisioned ahead of need, per `plans/docs/16-development-workflow.md` §16.2 — it was simply unused until now).

## Scope / Deliverables
- `internal/cases/workflow/` — `BreakLifecycleWorkflow`, `ApprovalWorkflow` (child workflow), and their Activities.
- `internal/cases/workflow/activities.go` — `AutoAssignActivity`, `ComputeSLAActivity`, `AuthorizeApprovalActivity`, `PersistTransitionActivity`, `EmitOutboxEventActivity`.
- `internal/cases/migration/backfill.go` — one-time backfill program starting Temporal workflows for existing open Break rows.
- Temporal worker registration inside `cmd/coreapi` (see the deployment-topology note below — do not create a new `cmd/case-worker` binary).
- Updated implementation of whatever `CaseLifecycle`-shaped interface task 13 exposed, now backed by Temporal instead of direct Postgres mutation.
- `migrations/00xx_case_feature_flag.sql` if `tenant_feature_flags` (already in the schema per `01-multi-tenancy.md` §2.3) doesn't yet have a `cases.temporal_enabled` row-level flag mechanism usable per-tenant.

## Design Reference
- `plans/docs/05-case-management.md` §6.1 (lifecycle + Temporal), §6.2 (auto-assignment as Activity), §6.3 (SLA timers), §6.4 (`ApprovalWorkflow`, maker-checker), §6.6 (root-cause taxonomy — unchanged).
- `plans/docs/03-canonical-data-model.md` (`Break`/`Case` fields, `temporal_workflow_id`).
- `plans/docs/10-observability-reliability.md` §11.5 (expand-contract migration discipline — apply it to the old-code removal, not just schema changes).

**Deliberate scoping call — flag for reconciliation with other tasks**: `plans/docs/00-overview-and-architecture.md` §1.1 lists "Case/Workflow Service" among services split into separate deployables from day one, but `plans/docs/16-development-workflow.md` §16.1's `cmd/` layout has no separate case-worker binary — `internal/cases` lives inside `cmd/coreapi`. This task follows the more concrete `16-development-workflow.md` repo layout: the Temporal worker (task-queue poller) runs as a goroutine started inside `cmd/coreapi`'s `main.go` alongside the HTTP/Connect-RPC server, not as a separate deployable. Extraction into its own binary remains available later per §1.1's own extraction-trigger rule (measured RPS/CPU) — this task does not block that, it just doesn't do it prematurely.

## Implementation Notes

### Workflow definition
```go
type BreakLifecycleWorkflowInput struct {
    BreakID       string
    TenantID      string
    InitialStatus BreakStatus // OPEN for new cases; ASSIGNED/IN_PROGRESS/... when resuming via backfill
    OpenedAt      time.Time
    SLADueAt      *time.Time // nil if not yet computed
}

func BreakLifecycleWorkflow(ctx workflow.Context, in BreakLifecycleWorkflowInput) error
```
- Workflow ID is deterministic: `case-{break_id}` — required for idempotent (re)starts during backfill.
- If `InitialStatus == OPEN`: execute `AutoAssignActivity` first, transition to `ASSIGNED`.
- If `InitialStatus` is anything past `OPEN` (backfill/resume case): **do not** re-run auto-assignment — jump straight into the signal-await loop at the corresponding point. Getting this wrong means re-assigning cases a human already has in progress, a real user-facing bug.
- Main body is a `workflow.Selector` loop awaiting Signals: `assign`, `comment`, `transition`, `submit_for_approval`, `escalate`, `resolve`, `write_off`, `reopen`. Port task 13's existing transition-validity table (which states are legal from which) into the signal handlers rather than inventing a new one — the state diagram in §6.1 must match what task 13 already implemented.
- All side effects (Postgres writes, outbox emission) happen inside Activities, never directly in workflow code — Temporal workflows must stay deterministic/replayable.
- SLA timers: `workflow.NewTimer` for the 75%-elapsed warning checkpoint and the 100%-elapsed breach checkpoint, computed from `SLADueAt`. Breach fires `AuthorizeApprovalActivity`... no — breach fires priority bump + escalation + an `EmitOutboxEventActivity` for `break.sla_breached` (task 21 delivers it).
- `submit_for_approval` / `write_off` signals: `workflow.ExecuteChildWorkflow(ctx, ApprovalWorkflow, ...)`, block on its result before proceeding.

### ApprovalWorkflow (maker-checker, §6.4)
```go
type ApprovalWorkflowInput struct {
    CaseID            string
    MakerUserID       string
    Action            string // "confirm_low_confidence_match" | "write_off" | "rule_activation"
    RequiredApprovals int    // configurable multi-level chains, e.g. 2 for write-offs > $1M
}
```
- Waits on repeated `approve`/`reject` signals until `RequiredApprovals` distinct approvers have signed, or any `reject`.
- Enforces `maker != checker` by calling `AuthorizeApprovalActivity(ctx, subject, resource)` on each incoming approve signal, rejecting a second signal from the same user as a duplicate/no-op, not a second valid approval.
- **This task's `AuthorizeApprovalActivity` implementation is a simple role-check stub** (e.g. "does this user have the Approver role and is `subject.user_id != resource.maker_user_id`") — core task 23 later swaps the activity's internals for a real OPA/Rego-backed decision. Keep the Activity's function signature stable across that swap; that is the seam task 23 is expected to use.
- Automatic reminders: a repeating timer inside the child workflow re-emits a `case.approval_requested` outbox event at a configurable interval while pending.

### Auto-assignment (§6.2) — this is net-new logic, not a relocation
MVP task 13 only supported **manual** assignment (per `plans/docs/11-scalability-roadmap.md` §12.2 Phase 0). The routing logic itself — round-robin, load-balanced (fewest open cases), skill/account-based routing, root-cause→team mapping — does not exist yet and must be built here as `AutoAssignActivity`, consulting a tenant-scoped versioned `TeamRoutingConfig` (new JSONB config, tenant-scoped, similar shape to `MatchRule` versioning).
```go
func AutoAssignActivity(ctx context.Context, in AutoAssignInput) (AutoAssignResult, error)
```

### Backfill (`temporal_workflow_id` migration path)
1. Select all `Break` rows where `temporal_workflow_id IS NULL AND status NOT IN (RESOLVED, WRITTEN_OFF)` — only open cases need a live workflow; leave resolved/written-off historical rows with `temporal_workflow_id = NULL` permanently (cheaper, and replaying closed-case history through Temporal is not required by the design).
2. For each, start `BreakLifecycleWorkflow` with `InitialStatus` set to the row's **current** status (not `OPEN`) and existing `assigned_to`/`sla_due_at` carried through as workflow input so the workflow doesn't redo work a human already did.
3. Persist the returned Temporal workflow ID into `Break.temporal_workflow_id` in the same operation (idempotent upsert keyed by the deterministic workflow ID, safe to re-run the backfill program).
4. Cutover: gate new-Break creation and all transition entrypoints on a per-tenant `cases.temporal_enabled` feature flag (reuse `tenant_feature_flags` from task 04/01 §2.3) so migration can roll out tenant-by-tenant rather than a single big-bang flag day. Only remove task 13's old Postgres-only state-machine code once all tenants in an environment are flipped and backfilled — do this removal as its own follow-up commit after the flag-flip is verified, not bundled into the same change as the flip (expand-contract discipline, §11.5).

### Interface preservation
Whatever interface task 13 exposed to its callers (API handlers, the ingestion pipeline's break-creation call site) must keep working unchanged. If task 13's actual method names/signatures differ from what's assumed here, adapt this task's Temporal-backed implementation to satisfy task 13's existing signatures exactly — this task changes what happens *behind* the interface, not the interface itself.

## Non-Goals / Guardrails
- Do not build OPA/RBAC (task 23) — `AuthorizeApprovalActivity` is a stub here by design, with a documented seam for task 23 to fill in later without touching the workflow's signal contract.
- Do not touch matching engine or rule DSL code.
- Do not build a separate `cmd/case-worker` binary — see the deployment-topology note above.
- Do not backfill Temporal workflows for already-terminal (`RESOLVED`/`WRITTEN_OFF`) Break rows.
- Do not delete task 13's old code in the same change that flips the feature flag — sequence them as separate, verifiable steps.

## Definition of Done
- Unit tests using `go.temporal.io/sdk/testsuite` replaying workflow histories for: happy path (open→assign→resolve), escalation, maker-checker approval (single and multi-level), reject-then-resubmit, and backfill-resume starting from `ASSIGNED`/`IN_PROGRESS`.
- Integration test using Temporal's time-skipping test environment to verify SLA timer firing at 75%/100% without real wall-clock sleeps.
- Backfill script tested against a fixture DB containing several open, non-migrated `Break` rows — asserts `temporal_workflow_id` gets populated and the workflow is queryable, and that already-assigned cases are not re-assigned.
- Task 13's existing caller-facing tests (API handlers, ingestion break-creation call site) pass unmodified against the new Temporal-backed implementation — this is the explicit contract-preservation check and should be treated as a hard requirement, not a nice-to-have.

## Common Pitfalls
- Performing Postgres writes or webhook/outbox emission directly inside workflow function bodies instead of via Activities — breaks Temporal's determinism/replay guarantees.
- Non-deterministic workflow IDs, causing duplicate workflow starts during backfill instead of idempotent resumption.
- Re-running `AutoAssignActivity` for backfilled cases that already have a human assignee — a real regression a user would notice immediately.
- Silently changing task 13's public interface and forcing unrelated call sites to be rewritten.
- Treating the maker-checker check as a UI-layer convention instead of an enforced workflow/activity-level gate — the design is explicit this must not be bypassable by calling the API directly.
- Skipping the per-tenant flag-based rollout and attempting a single-cutover migration across all tenants at once.
