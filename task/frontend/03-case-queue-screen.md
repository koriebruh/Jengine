# Task 03: Case/Break Queue Screen

## Goal
Build the analyst's primary work-list: a filterable, sortable, virtualized table of open breaks/cases, with row-selection-driven bulk actions (assign/comment/resolve). This is one of the three screens explicitly named as MVP-critical (plans/docs/14-dashboard-frontend.md §14.4 — "enough for design-partner analysts to work breaks without needing the API directly"). MVP scope updates via polling only; SSE live-updates are a distinct later task (frontend task 10) that retrofits this screen without rebuilding it.

## Prerequisites
- Frontend task 01 (bootstrap) and frontend task 02 (API client, TanStack Query, auth) — this screen is a pure consumer of both.
- Core task 05 (canonical domain models and repositories) — defines the `Break`/`Case` shape this screen renders.
- Core task 13 (case/break lifecycle, MVP state machine) — provides the actual status values (`OPEN → ASSIGNED → IN_PROGRESS → PENDING_APPROVAL → RESOLVED`, plus `ESCALATED`/`WRITTEN_OFF`) and the assign/comment/resolve mutations this screen calls.
- Core task 15 (REST API layer, MVP) — the concrete `/v1/tenants/{id}/breaks` list/bulk-action endpoints.

## Scope / Deliverables
- `web/components/data-table/data-table.tsx` — the shared virtualized TanStack Table wrapper (per plans/docs/14-dashboard-frontend.md §14.3, this primitive is reused by frontend tasks 05, 06, 11 later — build it generically here, not case-specific). Supports: column sort, column visibility toggle, row selection (checkbox column), virtualized rows (`@tanstack/react-virtual` or TanStack Table's built-in virtualization pattern) for large lists.
- `web/components/data-table/toolbar.tsx` — generic filter-bar/bulk-action-bar slot shared by the same table consumers.
- `web/components/case/status-badge.tsx` — colored badge per `Break.status` value, shared with frontend task 04.
- `web/components/case/sla-countdown-chip.tsx` — renders `sla_due_at` as a countdown/overdue indicator, shared with frontend task 04.
- `web/app/(dashboard)/cases/page.tsx` — the Case Queue screen itself: filter controls (account, priority, root-cause, assignee, status), the data table (columns: status badge, break_type, account, amount_at_risk + currency, assignee, SLA chip, opened_at), row selection, bulk-action toolbar (Assign to..., Add comment, Resolve) appearing when rows are selected.
- `web/lib/api/endpoints/cases.ts` (extends frontend task 02's stub if not already present) — `listBreaks(filters, pageToken)`, `bulkAssign(breakIds, userId)`, `bulkComment(breakIds, text)`, `bulkResolve(breakIds, resolution)`.
- `web/lib/query/keys.ts` additions — `queryKeys.breaks.list(filters)` invalidated on every bulk mutation's success.
- `web/components/case/case-filters.tsx` — the filter-bar controls specific to breaks (separate from the generic `toolbar.tsx` shell).

## Design Reference
- plans/docs/14-dashboard-frontend.md §14.2 screen 2 — exact feature list: filterable/sortable/virtualized table by account/priority/root-cause/assignee/SLA countdown; row-selection bulk assign/comment/resolve; "live-updates via SSE" is explicitly the *target* behavior, not this task's — see §14.4 Phase 0: "polling not SSE" at MVP.
- plans/docs/15-end-to-end-flows.md §15.1 steps 13–16 — this is the flow this screen is the UI for: breaks are created when transactions stay `UNMATCHED` after the rule chain; a `break.created` webhook fires (not consumed by this MVP screen — webhooks are V1, frontend task 09/core task 21); an analyst works the queue from here into the Case Detail screen (frontend task 04).
- plans/docs/05-case-management.md §6.1 (lifecycle states), §6.2 (auto-assignment — this screen shows the *result* of auto-assignment, it doesn't implement routing logic), §6.6 (root-cause taxonomy — filter dropdown values come from this list plus tenant-configured extensions).
- plans/docs/03-canonical-data-model.md §4.1 `Break`/`Case` fields — use these field names verbatim in table columns and filter params.

## Implementation Notes
- Data fetching: `useQuery` with `refetchInterval` (e.g. 10–15s) as the MVP polling mechanism — pick an interval and document it as a constant (`CASE_QUEUE_POLL_INTERVAL_MS`) in one place so frontend task 10 can find and replace this exact mechanism later without hunting for magic numbers scattered across the component.
- Table state: sort/filter/column-visibility state lives in the URL search params (Next.js `useSearchParams`/`useRouter`) so a filtered/sorted view is shareable/bookmarkable and survives a refresh — do not keep this only in local component state.
- Bulk actions: on selecting rows, show a floating/sticky action bar (per §14.2 "row-selection → bulk assign/comment/resolve"). Each bulk action is a `useMutation` that calls the corresponding `lib/api/endpoints/cases.ts` function with the array of selected break IDs, then invalidates `queryKeys.breaks.list` on success and clears selection. Use optimistic status updates for `bulkResolve`/`bulkAssign` (flip the row's status/assignee locally immediately, roll back on error) since these are the most common repeated actions and perceived latency matters for analyst throughput — per §14.1's optimistic-update rationale.
- SLA countdown chip states: green (>25% time remaining), amber (<25%, matches the "75% elapsed" warning checkpoint from plans/docs/05-case-management.md §6.3), red (overdue). This is presentation-only in MVP — the actual SLA timer/escalation logic lives in the backend (core task 13 MVP, replaced by Temporal timers in core task 20 V1); this screen only renders `sla_due_at` and computes remaining time client-side for display, it does not own SLA state.
- Empty state: "No open breaks match these filters" with a clear-filters action, distinct from the loading skeleton state and from a true zero-breaks-for-tenant state (different copy for "great, nothing's broken" vs. "no matches for your filter").
- Error state: a full-table error state (fetch failed) distinct from a row-level bulk-action failure toast — don't conflate the two.
- Root-cause and assignee filter dropdowns need their own lookups (tenant's user list, tenant's root-cause taxonomy) — add small supporting endpoint calls in `lib/api/endpoints/cases.ts` or a `tenants.ts` module rather than hardcoding the default taxonomy list from §6.6 as a static constant (tenants can extend it).

## Non-Goals / Guardrails
- Do not implement SSE or any WebSocket/EventSource connection — that is frontend task 10, which explicitly retrofits this exact screen. Polling via `refetchInterval` is the correct and complete MVP mechanism; do not half-build a socket connection "for later."
- Do not build the Case Detail view (linked transactions, timeline, root-cause tagging, attachments) — that is frontend task 04. This screen only navigates to it (row click → `/cases/{id}`).
- Do not implement the maker-checker approval UI for write-offs — that's part of frontend task 04's scope (and even there, only against whatever MVP case-lifecycle API exists; full Temporal `ApprovalWorkflow` UI is V1).
- Do not build the Match Review Queue here even though it's visually similar (candidate-row-per-suggested-match) — that's frontend task 05, with materially different columns (confidence breakdown) and actions (confirm/reject, not assign/resolve).
- Do not implement SLA timer/escalation logic client-side beyond display formatting — SLA state transitions are a backend concern.

## Definition of Done
- Against a real (or realistically mocked, per frontend task 02's MSW setup) API: the table loads, virtualizes correctly with a large (1000+ row) fixture without jank, sort/filter/pagination all work and reflect in the URL.
- Selecting multiple rows and running each bulk action (assign/comment/resolve) succeeds end-to-end, with visible optimistic feedback and correct rollback on a simulated failure.
- SLA chips render correct color states for fixture data spanning all three thresholds (healthy/warning/overdue).
- Manual verification: filter by root cause, sort by SLA countdown ascending, select 3 rows, bulk-assign, confirm the list reflects the new assignee after the next poll tick without a full page reload.
- QA issues go in the single root-level `QA_REPORT.md`, open items only — not a checklist appended to this file.

## Common Pitfalls
- Building a bespoke table implementation instead of the shared `components/data-table/` primitive — this directly contradicts §14.3's explicit intent to avoid "four bespoke table implementations" across Case Queue/Match Review/Connector Monitor/Audit Viewer.
- Keeping filter/sort state in component `useState` only — breaks shareable URLs and loses state on refresh, a common regression in list screens.
- Implementing an actual polling+socket hybrid "just in case SSE is easy to add now" — creates half-finished real-time code for frontend task 10 to untangle; keep this screen strictly polling-only.
- Treating `PENDING_APPROVAL` as equivalent to `RESOLVED` in status-badge coloring — these are distinct lifecycle states per §6.1 and must be visually distinguishable.
- Hardcoding root-cause taxonomy as a static frontend constant — the taxonomy is tenant-extensible per §6.6; fetch it, don't bake it in.
