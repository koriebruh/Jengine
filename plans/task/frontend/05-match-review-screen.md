# Task 05: Match Review Screen

## Goal
Build the "suggested matches" inbox: one row per candidate match the engine scored between `suggest` and `auto_match` thresholds, each showing a transparent per-field confidence breakdown (not just a single opaque score), with one-click confirm/reject and bulk-confirm for high-confidence batches. This screen is a named competitive differentiator — plans/docs/12-competitive-differentiation.md calls out "transparent confidence-score breakdowns" directly against ReconArt's "opaque internals, hard to debug 'why didn't this match'" weakness. Do not build a black-box score-only view; the per-field breakdown is the point of this screen, not a nice-to-have.

## Prerequisites
- Frontend task 01, frontend task 02.
- Frontend task 03 — reuses `components/data-table/` (the shared virtualized table primitive) built there.
- Core task 10 (matching engine core library) and core task 11 (matching rule DSL) — define the `MatchResult`/`MatchResultLine` shapes and the per-field scoring breakdown this screen must render.
- Core task 12 (matching batch worker) — produces the `SUGGESTED` `MatchResult` rows this screen lists.
- Core task 15 (REST API layer, MVP) — the `/v1/tenants/{id}/match-results` list/confirm/reject endpoints.

## Scope / Deliverables
- `web/app/(dashboard)/matches/page.tsx` — the screen: filterable/sortable table of `SUGGESTED` `MatchResult` rows, confidence score column, expandable per-row breakdown, row-selection bulk-confirm bar.
- `web/components/match/confidence-breakdown.tsx` — the core differentiator component: given a `MatchResult` and its scoring inputs, renders each scored field (per plans/docs/04-matching-engine.md §5.1 `scoring:` list — e.g. `reference` via jaro_winkler, `counterparty_ref` via levenshtein_normalized, `base_amount` via numeric_closeness, `value_date` via date_proximity) with its individual similarity value, its configured weight, and its contribution to the composite score — not just the final 0–1 number. Render as an expandable row-detail panel or a hover/click popover (pick one consistent interaction, not both) showing a small per-field bar/table: field name, raw similarity (0–1), weight, weighted contribution.
- `web/components/match/match-candidate-row.tsx` — row renderer: source transaction summary, target transaction summary (or multiple, for one-to-many/many-to-many candidates), composite confidence score badge (color-coded by proximity to `auto_match` threshold), confirm/reject icon buttons.
- `web/components/match/linked-transaction-pair.tsx` — compact side-by-side rendering of the source/target transaction(s) being compared (amount, date, reference, counterparty) so the analyst can visually eyeball the match without leaving the row.
- `web/lib/api/endpoints/matches.ts` — `listSuggestedMatches(filters, pageToken)`, `confirmMatch(matchResultId)`, `rejectMatch(matchResultId, reason)`, `bulkConfirmMatches(matchResultIds)`.

## Design Reference
- plans/docs/14-dashboard-frontend.md §14.2 screen 4 — "one candidate per row, confidence score with per-field similarity breakdown... one-click confirm/reject, bulk-confirm for high-confidence batches."
- plans/docs/04-matching-engine.md §5.1 (scoring config shape — this is exactly the structure the breakdown component must reflect: `field`, `method`, `weight`, `min_similarity`), §5.4 (`>= suggest, < auto_match` is this screen's population; the design explicitly says "score + field breakdown shown, one-click confirm/reject" and "avoid alert fatigue" by not surfacing anything below `suggest` by default).
- plans/docs/12-competitive-differentiation.md — "Opaque internals, hard to debug 'why didn't this match'" row: Jengine's answer is "transparent confidence-score breakdowns." This is the design intent this screen exists to fulfill; treat the breakdown component as non-negotiable scope, not a stretch goal.
- plans/docs/15-end-to-end-flows.md §15.1 step 12–13 — a `SUGGESTED` match is not a break; it only becomes one if explicitly rejected here. Rejecting from this screen is what creates a `Break` — this screen's reject action has a real downstream effect on the Case Queue (frontend task 03), not just a local dismiss.

## Implementation Notes
- Confidence badge color bands: use the same three-tier logic as the backend's `auto_match`/`suggest` thresholds (this screen only ever shows `suggest <= score < auto_match` rows, but within that band, differentiate visually — e.g. closer to `auto_match` reads as "high confidence, safe to bulk-confirm" vs. closer to `suggest` reads as "needs a closer look"). Don't hardcode global threshold numbers in the frontend — read the effective thresholds from the `MatchRule` that produced each result (a rule's thresholds are tenant/rule-configurable per §5.1) so the color bands are correct even if a tenant has customized them, rather than assuming the example `0.92`/`0.65` from the design doc are universal constants.
- Per-field breakdown data source: confirm with core task 15's actual response shape whether the API returns per-field similarity components directly on `MatchResult`/`MatchResultLine`, or whether this requires a supplementary call. If the MVP API doesn't yet expose per-field breakdown data (a real risk, since this is new UI-driven requirements pressure on the API per plans/docs/14-dashboard-frontend.md's intro note that "screen requirements shape API design decisions"), flag this as a blocking API gap immediately rather than fabricating placeholder numbers in the UI — a fake breakdown would actively undermine the transparency differentiator this screen exists for.
- Bulk-confirm: select rows (reuse `components/data-table/`'s row-selection), a "Confirm N selected" action bar button appears, calls `bulkConfirmMatches`. Optimistic-remove confirmed rows from the list on success (they leave the `SUGGESTED` queue once confirmed). On partial failure (some IDs succeed, some don't — check the actual bulk endpoint's response contract), surface which ones failed and why, don't silently swallow partial failures as a full success toast.
- One-to-many / many-to-many candidates: a `MatchResult` can link N source transactions to M target transactions via `MatchResultLine` rows (per plans/docs/03-canonical-data-model.md §4.1). The row/breakdown UI must handle this — `linked-transaction-pair.tsx` should actually be capable of rendering a list on either side, not assume exactly one-to-one (name it accordingly in code even if the initial visual treatment optimizes for the common one-to-one case).
- Filtering: by account pair, by rule (which `MatchRule` produced the candidate), by score range, by cardinality type — mirror the filter conventions established in frontend task 03 for consistency (URL-driven filter state).
- Reject flow: rejecting should prompt for an optional reason (feeds `Break.root_cause_category` potentially, or at minimum an audit note) — check whether core task 13/15's reject endpoint requires or accepts this; don't force a reason if the backend doesn't model one yet, but don't build a UI that silently discards a reason the user typed either.

## Non-Goals / Guardrails
- Do not build the manual "wide search" candidate lookup — that's frontend task 04's Case Detail screen scope (searching for a match for an already-existing break is different from reviewing the engine's own suggested-match queue).
- Do not implement SSE or live-updating of this queue — polling only at MVP, per plans/docs/14-dashboard-frontend.md §14.4; frontend task 10 retrofits this screen specifically (it's named in that task's scope).
- Do not build the Rule Builder UI or expose rule editing from this screen, even though rule thresholds are referenced here — this screen only *reads* rule threshold values to color-code scores; editing rules is frontend task 08 (V1).
- Do not collapse the per-field breakdown into a single "confidence score: 78%" number anywhere in this screen — that is the exact anti-pattern this screen is built to avoid.
- Do not build ML-based scoring explanation UI (e.g. SHAP-style feature attribution) — the composite score is a deterministic weighted sum per §5.1/§5.3 at MVP and V1; ML-based scoring is explicitly V2 (plans/docs/04-matching-engine.md §5.3), out of scope entirely for this task.

## Definition of Done
- Against real/mocked `SUGGESTED` match data: every row's confidence breakdown correctly sums the displayed weighted per-field contributions to (approximately) the row's composite score — verify the arithmetic actually reconciles, don't just render disconnected numbers.
- Confirm and reject both work individually and reflect immediately (optimistic) with correct rollback on simulated failure.
- Bulk-confirm across a multi-row selection succeeds, with partial-failure handling verified against a simulated mixed-result response.
- Rejecting a candidate is verified (via the API response or a follow-up query) to actually result in a new `Break` appearing in the Case Queue's data — confirming the cross-screen effect described in plans/docs/15-end-to-end-flows.md §15.1 step 13 is real, not just locally simulated.
- One-to-many/many-to-many fixture data renders correctly (multiple transactions on one side visible and legible, not truncated to one).
- QA issues in the single root-level `QA_REPORT.md`.

## Common Pitfalls
- Building a simplified score-only view "to ship faster" and treating the field-breakdown component as a follow-up — this is the named differentiator; it is core scope for this task, not a stretch goal to defer.
- Hardcoding `0.92`/`0.65` as global thresholds instead of reading each result's originating rule's actual configured thresholds — silently wrong the moment a tenant customizes a rule.
- Assuming one-to-one cardinality throughout the component tree and having to retrofit one-to-many rendering later — design `linked-transaction-pair.tsx` for a list on both sides from the start.
- Conflating this screen's reject action with the Case Detail screen's write-off/resolve actions — they are different backend operations on different entities (`MatchResult` vs. `Break`) even though rejection causes a `Break` to appear.
- Fabricating placeholder per-field breakdown numbers if the MVP API doesn't return them yet, instead of flagging the gap — a fake transparency feature is worse than an honest "not yet available" state.
