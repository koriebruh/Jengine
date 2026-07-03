# Task 04: Case Detail Screen

## Goal
Build the single-break workspace: an analyst lands here from the Case Queue to see everything about one break — its linked transactions, its full comment/audit history interleaved in one timeline, and the actions that move it through its lifecycle (assign, escalate, resolve, write-off) plus root-cause tagging and attachment upload. This is the second of the three MVP-critical screens (plans/docs/14-dashboard-frontend.md §14.4).

## Prerequisites
- Frontend task 01, frontend task 02.
- Frontend task 03 (Case Queue) — reuses `components/case/status-badge.tsx` and `components/case/sla-countdown-chip.tsx` built there; this task should not redefine them.
- Core task 05 (canonical domain models) — `Break`, `CaseComment`, `CaseAuditEvent`, `Transaction`, `MatchResultLine` shapes.
- Core task 13 (case/break lifecycle, MVP state machine) — the assign/escalate/resolve/write-off endpoints this screen calls exist here, as a Postgres-backed simple state machine (not yet Temporal — see the seam note below).
- Core task 14 (audit logging) — the `CaseAuditEvent`/`AuditEvent` records this screen's timeline reads.
- Core task 15 (REST API layer, MVP).

## Scope / Deliverables
- `web/app/(dashboard)/cases/[caseId]/page.tsx` — the screen: header (break type, status badge, SLA chip, amount at risk, assignee), linked-transactions panel, interleaved timeline, action bar, root-cause tagger, attachment uploader.
- `web/components/case/timeline.tsx` — renders `CaseComment` and `CaseAuditEvent` records merged into one chronological feed (comments are user-authored rich text + attachments; audit events are system-generated structured transitions, e.g. "Status changed OPEN → ASSIGNED by auto-assignment"). Shared conceptually with frontend task 11's audit viewer but that task reads the *global* `AuditEvent` log across all cases, not one case's feed — do not merge these two components into one; they serve different query shapes.
- `web/components/case/linked-transactions.tsx` — table (can reuse `components/data-table/` from frontend task 03 in a simpler non-virtualized mode, since a single case's linked transaction count is small) of transactions tied to this break via `related_transaction_ids`, plus a "wide search" affordance (manual lookup for a low-score candidate the automated matcher didn't surface — per plans/docs/15-end-to-end-flows.md §15.1 step 16).
- `web/components/case/root-cause-tagger.tsx` — dropdown/combobox over the tenant's root-cause taxonomy (same source as frontend task 03's filter, reuse the lookup) with an inline "add custom category" affordance since the taxonomy is tenant-extensible.
- `web/components/case/attachment-upload.tsx` — file upload control posting to whatever object-storage-backed attachment endpoint core task 15 exposes for `CaseComment` attachments.
- `web/components/case/case-actions.tsx` — the action bar: Assign, Escalate, Add Comment, Resolve, Write Off. Each a `useMutation` against `lib/api/endpoints/cases.ts`.
- `web/lib/api/endpoints/cases.ts` additions — `getBreak(id)`, `getBreakTimeline(id)`, `addComment(id, payload)`, `escalateBreak(id, reason)`, `resolveBreak(id, resolution)`, `writeOffBreak(id, justification)`, `uploadAttachment(caseId, file)`.

## Design Reference
- plans/docs/14-dashboard-frontend.md §14.2 screen 3 — exact scope: break metadata, linked transactions, interleaved comment/audit timeline, assign/escalate/approve actions, root-cause tagging, attachment upload.
- plans/docs/05-case-management.md §6.1 (lifecycle diagram — this screen's action bar must only expose transitions valid from the break's *current* status; don't render a "Resolve" button on an already-`RESOLVED` case), §6.4 (maker-checker approval — see seam note below), §6.5 (comment vs. audit event distinction — comments are free-text + attachments + @mentions; audit events are structured system records; both feed the same timeline component but are visually distinguishable), §6.6 (root-cause taxonomy).
- plans/docs/15-end-to-end-flows.md §15.1 step 16 — the concrete interaction this screen supports: "opens the Case Detail screen, reviews linked transactions, optionally runs a manual wide search for a low-score candidate, adds comments, tags a root cause, and resolves — or writes off, which routes through the maker-checker `ApprovalWorkflow` child workflow."
- plans/docs/03-canonical-data-model.md §4.1 — `CaseComment`/`CaseAuditEvent` fields (`actor`, `event_type`, `payload jsonb`, `created_at`, append-only).

## Implementation Notes — the MVP vs. V1 approval seam
plans/docs/05-case-management.md §6.4 describes write-offs (and other financially consequential actions) routing through a Temporal **child workflow** (`ApprovalWorkflow`) with `maker != checker` enforced at the workflow level. That Temporal-backed workflow is explicitly core task 20, **V1 scope** — plans/docs/11-scalability-roadmap.md §12.2 Phase 0 confirms MVP case management is "basic lifecycle (simple state machine + Postgres, no Temporal yet)." So at the time this screen is built (MVP), core task 13's write-off/resolve endpoints will *not* yet enforce maker-checker at the workflow level.

Build this screen's "Write Off" and "Resolve" actions against whatever core task 13 actually exposes for MVP (most likely: a status transition endpoint that records `resolved_by`/`written_off_by` directly, possibly with an application-layer check that the resolver isn't the same user who opened/escalated the case, if core task 13 implements even a lightweight maker-checker check — confirm the actual endpoint contract when core task 13 lands rather than assuming). Design the action bar so that when core task 20's real `ApprovalWorkflow` lands in V1, the UI seam is: a write-off submission moves the case to `PENDING_APPROVAL` and a *separate* approve/reject action (gated to a different user than the submitter) appears — **stub this state and button now** if core task 13's MVP API already models `PENDING_APPROVAL` as a status (per the lifecycle diagram in §6.1, it's part of the state machine from MVP, not added in V1), even if the actual two-person enforcement doesn't bite until core task 20. Do not build a fake/local-only maker-checker check in the frontend (e.g., hiding the approve button if `currentUser.id === submitted_by` is fine defensively, but never treat that hidden button as the security boundary — the backend enforces it, this UI only reflects it).

## Implementation Notes — general
- Timeline sort: strictly chronological ascending (oldest first, matching how conversations/audit trails are conventionally read), with a "jump to latest" affordance if the list is long. Merge client-side after fetching both `CaseComment` and `CaseAuditEvent` collections (or a single combined endpoint if core task 15 provides one — check before building two separate fetches only to merge them, as a combined server-side feed is preferable if available).
- Attachment upload: show upload progress, and on the timeline, a comment with attachments renders file chips with size/type, not raw links — but do not build a file preview/viewer; a download link is sufficient for MVP.
- Root-cause tagging: this is a mutation on the `Break` record (`root_cause_category`), separate from adding a comment — don't conflate "tag a root cause" with "leave a comment about the root cause," though a comment often accompanies the tag in practice.
- Action bar buttons are conditionally rendered/disabled based on `Break.status` — encode the valid-transition map (from §6.1's diagram) as a small lookup table (`VALID_TRANSITIONS: Record<BreakStatus, BreakStatus[]>`) rather than scattering `if (status === ...)` checks across the component.
- "Wide search" (manual candidate lookup): a simple search input over the tenant's unmatched transactions (calls a search endpoint, likely on `lib/api/endpoints/matches.ts` or `cases.ts` depending on what core task 15 exposes) with a manual "link this transaction to this case" action — keep this simple (text search + result list + link button), it is not the Match Review Queue's suggested-match UI (frontend task 05).

## Non-Goals / Guardrails
- Do not build the Temporal `ApprovalWorkflow`'s actual multi-level approval-chain UI (configurable N-approvers, automatic reminders) — that's core-side V1 logic (core task 20); this screen only needs the seam described above.
- Do not implement OPA/ABAC-based conditional action visibility (e.g., "hide Approve if this user's policy would reject it") — that's V2 per plans/docs/14-dashboard-frontend.md §14.4. Status-based (not permission-based) button gating is in scope; role/policy-based gating is not.
- Do not build the searchable global Audit Viewer (frontend task 11) — this screen's timeline is scoped to one case only.
- Do not implement SSE for live timeline updates — polling (if any refresh is needed at all beyond navigating back to this page) follows the same MVP convention as frontend task 03; frontend task 10 retrofits SSE onto the Case Queue and Match Review screens specifically, not explicitly this one (revisit only if frontend task 10 says otherwise when it's built).
- Do not build a rich-text WYSIWYG editor for comments beyond what's actually needed (plain multiline text + attachment picker is sufficient for MVP; don't add a markdown/rich-text engine dependency speculatively).

## Definition of Done
- Navigating from the Case Queue (frontend task 03) to a case detail page loads real metadata, linked transactions, and a correctly-interleaved, chronologically-ordered timeline.
- Every action button in the action bar is visible/disabled correctly per the break's current status (verify across fixture cases in each of `OPEN`/`ASSIGNED`/`IN_PROGRESS`/`PENDING_APPROVAL`/`RESOLVED`/`ESCALATED`/`WRITTEN_OFF`).
- Adding a comment (with an attachment) appears in the timeline without a full page reload; root-cause tagging persists and reflects on return visits and in the Case Queue's root-cause filter (frontend task 03).
- Resolve and Write Off both succeed against the MVP backend's actual endpoint contract (confirm the real request/response shape against core task 13 once it exists — do not assume a shape and leave it unverified).
- QA issues go in the single root-level `QA_REPORT.md`.

## Common Pitfalls
- Assuming core task 20's Temporal `ApprovalWorkflow` already exists and building a full multi-approver UI against an API that doesn't exist yet at MVP — build against the real MVP contract, with the seam noted above.
- Silently treating `PENDING_APPROVAL` as unreachable at MVP and omitting it from the status lookup table — it's in the lifecycle diagram from day one per §6.1, even if the enforcement mechanism upgrades later.
- Duplicating `status-badge.tsx`/`sla-countdown-chip.tsx` instead of importing frontend task 03's versions — creates two divergent implementations of the same visual element.
- Building this screen's timeline as comments-only, ignoring `CaseAuditEvent`s — the design explicitly requires both interleaved, since system-generated transitions (assignment, escalation, status changes) are as important to the audit narrative as human comments.
- Adding a rich-text editor dependency (TipTap/Slate/etc.) for what only needs to be a plain text area with attachment support at MVP.
