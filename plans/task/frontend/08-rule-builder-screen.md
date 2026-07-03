# Task 08: Rule Builder Screen (V1)

## Goal
Build the no-code visual rule builder that compiles down to the matching rule DSL, plus the backtesting sandbox panel that lets a Recon Manager see a proposed rule's projected impact before activating it, plus the pending-approval state display for the maker-checker gate rule changes go through. This is explicitly **V1, not MVP** — plans/docs/11-scalability-roadmap.md §12.2 Phase 0 states rules are "authored as raw YAML/JSON via API/CLI at MVP, no rule-builder UI yet." Do not begin this task until the MVP frontend tasks (01–07) are working end-to-end.

## Prerequisites
- Frontend task 01, frontend task 02.
- Frontend task 03 (reuses `components/data-table/` for the rule list/version history view).
- Core task 11 (matching rule DSL) — this task's entire scope is a UI over that DSL's schema; the visual builder's field/config surface must map 1:1 to the DSL's actual accepted structure, not a UI-team's reinterpretation of it.
- Core task 20 (Temporal workflow migration for case lifecycle, V1) — provides the real maker-checker `ApprovalWorkflow` primitive this screen's pending-approval state reflects (per plans/docs/15-end-to-end-flows.md §15.3 step 4, rule approval reuses the same maker-checker primitive as break write-offs).
- Core task 15 (REST API layer) must additionally expose whatever rule CRUD + backtest-execution endpoints core task 11 lands with — confirm the exact backtest request/response contract before building the sandbox panel's result rendering.

## Scope / Deliverables
- `web/app/(dashboard)/rules/page.tsx` — rule list (name, version, status: DRAFT/ACTIVE/ARCHIVED/PENDING_APPROVAL, account scope, last modified) with version history per rule.
- `web/app/(dashboard)/rules/new/page.tsx` and `web/app/(dashboard)/rules/[ruleId]/page.tsx` — the builder itself (create and edit share the same form component).
- `web/components/rule-builder/scope-picker.tsx` — source/target account-group selection (per plans/docs/04-matching-engine.md §5.1 `scope:` block).
- `web/components/rule-builder/blocking-key-editor.tsx` — composite blocking-key configuration (field + tolerance type: exact/date_window/numeric with absolute+percent bands), matching the `keys:` list shape.
- `web/components/rule-builder/scoring-weight-editor.tsx` — per-field scoring rows: field, similarity method (dropdown over the registered `ScoringFunc` names — jaro_winkler, levenshtein_normalized, numeric_closeness, date_proximity, etc.), weight slider, optional `min_similarity` floor — matching the `scoring:` list shape. Weights across rows should visually indicate whether they sum to something sane (e.g. a running total badge) without necessarily hard-blocking submission on a non-1.0 sum unless core task 11's DSL validation actually requires it — confirm the validation rule before enforcing it client-side.
- `web/components/rule-builder/threshold-editor.tsx` — `auto_match`/`suggest` threshold sliders with a visual preview of the resulting score bands (reuse the same color-banding logic from frontend task 05's confidence badges for visual consistency).
- `web/components/rule-builder/cardinality-and-aggregation.tsx` — cardinality selector (ONE_TO_ONE/ONE_TO_MANY/MANY_TO_ONE/MANY_TO_MANY) and, when non-1:1, the aggregation config (`max_group_size`, `sum_tolerance`).
- `web/components/rule-builder/backtest-panel.tsx` — the sandbox: historical date-range picker, "Run Backtest" action, results display (projected auto-match rate, false-positive risk estimate, break-volume delta vs. current active rule) — read-only, no `MatchResult` rows written (per plans/docs/15-end-to-end-flows.md §15.3 step 2, this is explicitly a dry-run).
- `web/components/rule-builder/approval-status-banner.tsx` — shows `DRAFT`/`PENDING_APPROVAL`/`ACTIVE`/`ARCHIVED` state, who submitted, and (if the current user is an authorized Approver and is not the submitter) an Approve/Reject action.
- `web/lib/api/endpoints/rules.ts` — `listRules()`, `getRule(id)`, `createRuleDraft(dsl)`, `updateRuleDraft(id, dsl)`, `submitForApproval(id)`, `approveRule(id)`, `rejectRule(id, reason)`, `runBacktest(ruleDraft, dateRange)`.

## Design Reference
- plans/docs/14-dashboard-frontend.md §14.2 screen 5 — full scope list (scope picker, blocking-key config, scoring-weight sliders, threshold sliders, backtesting sandbox, pending-approval state).
- plans/docs/04-matching-engine.md §5.1 — the DSL structure verbatim (this is the ground truth for every form field this builder needs; the YAML example in that section is effectively the builder's target output schema).
- plans/docs/04-matching-engine.md §5.4 — backtesting sandbox semantics: "replay a chosen historical date range read-only against the proposed rule, showing projected auto-match rate, false-positive risk, and break-volume delta before activating."
- plans/docs/15-end-to-end-flows.md §15.3 — the full rule authoring/activation flow this screen implements end-to-end: build in builder → backtest → submit (`DRAFT`) → different-user approval (`ACTIVE`, `effective_from=now`) → webhook fires → previous version archived (never deleted, full version history stays queryable).
- plans/docs/05-case-management.md §6.4 — the maker-checker primitive being reused here (same one break write-offs use); `maker != checker` is enforced at the workflow level, not UI convention — this screen's Approve button being hidden for the submitting user is a UX courtesy, not the actual enforcement boundary.

## Implementation Notes
- The builder should maintain one in-memory rule-draft object matching the DSL shape (`{ name, version, scope, match_cardinality, keys, scoring, thresholds, aggregation_rules, execution }`) that all the sub-editors (scope-picker, blocking-key-editor, etc.) read/write via shared form state (React Hook Form or a plain controlled-object pattern is fine — do not introduce a global state library for this; it's local-to-this-screen form state, consistent with plans/docs/14-dashboard-frontend.md §14.1's state philosophy).
- Do not implement DSL compilation/validation logic in the frontend beyond basic form-level validation (required fields, numeric ranges, weight sum sanity check) — the actual DSL parser/compiler is core task 11's `internal/matching/rules/dsl.go`. The builder's job is to construct a valid DSL document and submit it to the backend for authoritative validation, surfacing backend validation errors inline on the relevant form section, not to reimplement rule-compilation logic client-side.
- Backtest panel results rendering: confirm the actual response shape from `runBacktest` before building the results UI — the design doc names three metrics (projected auto-match rate, false-positive risk, break-volume delta) but doesn't specify their exact response field names/units; do not guess and hardcode a shape that might not match core task 11's real contract.
- Version history: rule versions are never deleted, only archived (§15.3 step 6) — the rule list/detail view should show the full version chain for a rule name, with the currently-active version clearly distinguished, and archived versions viewable (read-only) for audit purposes.
- Priority/execution mode: the `execution.priority` (rule-chaining order) and `execution.mode` (`[batch, streaming]`) fields from the DSL need their own small form controls — priority as a number input (with a note that lower runs first, matching the doc's convention), mode as a multi-select. At MVP+V1 boundary, note that `streaming` mode is only meaningful once core task 19 (streaming matching worker, V1) exists — the form should still allow selecting it (the DSL supports it from day one) even if the backend doesn't yet execute streaming-mode rules.

## Non-Goals / Guardrails
- Do not build the actual rule DSL parser, compiler, or backtest execution engine — those are core task 11's scope entirely. This screen only constructs/edits a DSL document and calls backend endpoints; it contains zero matching-logic code.
- Do not implement the multi-level approval chain configuration UI (e.g. "write-offs > $1M require two approvals" per plans/docs/05-case-management.md §6.4) unless core task 20 confirms rule approval uses that same configurable-chain mechanism — if it's a simple single-approver gate for rules specifically, don't over-build a generalized chain-config UI speculatively.
- Do not implement ML-based rule suggestions (e.g. "80% of breaks tagged Timing Difference would auto-match if date-window widened" per plans/docs/05-case-management.md §6.6) — that's a future/analytics feature, not named in this screen's V1 scope.
- Do not build this screen before frontend tasks 01–07 (MVP) are complete and verified — this is V1-numbered for a reason; starting it early risks building against an MVP API surface that doesn't yet have rule-DSL endpoints at all (rules are YAML/JSON-via-API/CLI only at MVP, meaning the underlying CRUD endpoints this screen needs may not exist until core task 11/20 land).
- Do not let weight-sum validation block submission if core task 11's actual DSL schema doesn't require weights to sum to 1.0 — verify the real constraint rather than assuming.

## Definition of Done
- A rule can be built end-to-end through the visual form (scope, blocking keys, scoring weights, thresholds, cardinality) and the resulting DSL document, when submitted, is accepted by core task 11's validation as well-formed (verify by round-tripping: create via the UI, then fetch the same rule and confirm the DSL structure matches what a hand-written YAML equivalent would produce).
- Backtest panel runs against a real historical range (or realistic fixture) and displays results without writing any real `MatchResult` rows (verify no side effects on the match data after a backtest run).
- Submitting a rule for approval moves it to `PENDING_APPROVAL`; approving as a different user moves it to `ACTIVE` and archives the prior active version; attempting to approve as the same user who submitted is rejected by the backend and the UI surfaces that rejection clearly (not just a generic error).
- Version history correctly shows the full non-deleted version chain for a rule.
- QA issues in the single root-level `QA_REPORT.md`.

## Common Pitfalls
- Reimplementing DSL validation logic (e.g. checking blocking-key tolerance types, scoring method names) as hardcoded frontend constants that drift from core task 11's actual registered `ScoringFunc` names — fetch the list of valid methods from the backend if it exposes one, rather than hardcoding a snapshot that goes stale.
- Building a fake "approve" button that only checks `currentUser.id !== submittedBy` client-side and treating that as sufficient security — the backend must reject same-user approval; the frontend check is only a UX nicety, and this task's Definition of Done explicitly requires verifying the backend rejects it.
- Starting this task in parallel with MVP frontend tasks to "save time" — the master build order explicitly gates V1 frontend work behind MVP frontend + backend completion; the rule DSL CRUD API this screen needs may simply not exist yet.
- Building the multi-level approval chain UI speculatively when core task 20 might implement rule approval as a simpler single-gate mechanism — confirm before building the more complex UI.
