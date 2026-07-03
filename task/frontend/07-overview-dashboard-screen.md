# Task 07: Reconciliation Overview Dashboard Screen

## Goal
Build the tenant's home screen: match-rate %, auto-match %, break volume/aging trend, SLA compliance gauge, and per-account-pair health tiles, giving a Recon Manager an at-a-glance read on reconciliation health without opening the Case Queue. This is the last of the seven MVP frontend tasks, but per plans/docs/14-dashboard-frontend.md's introductory note, dashboard requirements like "what needs a streaming/WS channel vs plain REST" and chart data shapes should already be understood before backend reporting endpoints are finalized — treat this task as consuming whatever MVP reporting the backend actually offers, not the full target ClickHouse-backed pipeline.

## Prerequisites
- Frontend task 01, frontend task 02.
- Core task 03 (database schema and migrations) — MVP relies on Postgres materialized views (see Design Reference) rather than ClickHouse; whichever core task actually creates those views (likely task 03 or a reporting-specific addition inside core task 15) must exist first — confirm the concrete view/endpoint names against core task 15's actual implementation rather than assuming the ClickHouse view names below apply verbatim at MVP.
- Core task 15 (REST API layer, MVP) — exposes whatever reporting/aggregate endpoints back this screen at MVP.

## ⚠️ Resolved Design Ambiguity: Postgres materialized views at MVP, not ClickHouse
plans/docs/14-dashboard-frontend.md §14.2 screen 1 states this screen is "backed by ClickHouse materialized views (`mv_match_rate_by_rule`, `mv_breaks_daily_aging`, `mv_sla_compliance`)." But ClickHouse itself is explicitly **V1 scope** — plans/docs/11-scalability-roadmap.md §12.2 Phase 0 states "Postgres only (defer ClickHouse; Postgres materialized views acceptable at MVP scale)," and core task 22 ("ClickHouse analytics pipeline") is V1-numbered. Since this frontend task is MVP-numbered (07), it must consume MVP-shaped data: **Postgres-backed materialized views or equivalent aggregate queries exposed via a REST reporting endpoint**, not a ClickHouse/GraphQL reporting gateway (that gateway is plans/docs/07-api-extensibility.md §8.4, also V1/V2). Build this screen's data-fetching layer against a reporting endpoint contract (e.g. `/v1/tenants/{id}/reports/overview`) that returns the same *shape* of data (match rate %, auto-match %, break aging buckets, SLA compliance %) regardless of whether the backend computes it from a Postgres materialized view (MVP) or ClickHouse (V1) — isolate this in one `lib/api/endpoints/reports.ts` module so the eventual ClickHouse cutover is a backend change only, invisible to this screen's components. Note this explicitly in that module's file header comment.

## Scope / Deliverables
- `web/app/(dashboard)/page.tsx` — the Overview Dashboard screen (replaces frontend task 01's placeholder): tile row (match rate %, auto-match %, open break count, SLA compliance %), break-aging trend chart, per-account-pair health tiles/table.
- `web/components/charts/` — shared chart primitives per plans/docs/14-dashboard-frontend.md §14.3, following the `dataviz` skill's design-system approach for color/accessibility consistency (invoke that skill's guidance when picking the palette/chart form — do not improvise ad hoc chart colors):
  - `trend-line.tsx` — match-rate/break-volume-over-time line chart.
  - `aging-histogram.tsx` — break-aging bucket bar chart (e.g. 0–24h / 1–3d / 3–7d / 7d+).
  - `sla-gauge.tsx` — SLA compliance % gauge/radial indicator.
- `web/components/dashboard/stat-tile.tsx` — the match-rate/auto-match/break-count/SLA percentage tiles (reusable small KPI-tile component).
- `web/components/dashboard/account-pair-health-table.tsx` — per-account-pair rows (source account, target account, match rate, open break count, last reconciled).
- `web/lib/api/endpoints/reports.ts` — `getOverviewSummary()`, `getBreakAgingTrend(dateRange)`, `getAccountPairHealth()`.

## Design Reference
- plans/docs/14-dashboard-frontend.md §14.2 screen 1 — tile/chart content list (see the resolved ambiguity above for the actual MVP data source).
- plans/docs/14-dashboard-frontend.md §14.1 — chart library choice (visx or Recharts) and the `dataviz` design-system approach for palette/accessibility consistency across all charts in the product.
- plans/docs/05-case-management.md §6.3 — SLA aging buckets and MTTR concept this dashboard's aging histogram and SLA gauge visualize (the same underlying metric the Case Queue's SLA chips render per-row; this screen aggregates it).
- plans/docs/11-scalability-roadmap.md §12.2 Phase 0/Phase 1 — confirms the Postgres-vs-ClickHouse phase boundary driving the resolved ambiguity above.

## Implementation Notes
- Use the `dataviz` skill before writing any chart code in this task — it governs categorical/sequential palette choice, tooltip/legend/axis conventions, and stat-tile layout; do not invent chart styling ad hoc, since §14.3 explicitly requires "one color/typography system so the dashboard reads as one product, not a bag of ad-hoc widgets."
- Data freshness: at MVP (Postgres materialized views, likely refreshed on a schedule or on-demand rather than true near-real-time CDC), don't imply a "live" freshness the backend doesn't provide — show a "last updated at HH:MM" timestamp sourced from the API response rather than a generic loading spinner masking staleness. Use `refetchInterval` polling (same convention as other MVP screens) at a coarser interval than the Case Queue's (dashboard aggregates change less frequently than individual break status) — e.g. 60s rather than 10–15s; make this a named constant, not a magic number.
- Break-aging trend chart: expects a bucketed time series from the API (`getBreakAgingTrend`) — design this hook so the date-range selector (e.g. last 7/30/90 days) is a simple control feeding the query params, not client-side data slicing of an over-fetched large payload.
- Per-account-pair health table: this table can reuse `components/data-table/` from frontend task 03 but likely doesn't need virtualization at typical account-pair counts — use the primitive's simple (non-virtualized) mode rather than skipping the shared component for a one-off table.
- Empty state: a brand-new tenant with zero statements ingested yet should show a clear "no reconciliation activity yet" state (linking to the Connector Monitor or documentation), not a broken/empty chart.
- Accessibility: charts need accessible tooltips/legends per §14.1 — don't rely on color alone to distinguish series/buckets; the `dataviz` skill covers this directly.

## Non-Goals / Guardrails
- Do not build against ClickHouse or a GraphQL reporting gateway — both are V1/V2 (core task 22, plans/docs/07-api-extensibility.md §8.4). Build strictly against the MVP REST reporting endpoint contract, isolated in `lib/api/endpoints/reports.ts` for a clean later swap.
- Do not implement advanced trend/drill-down analytics views (multi-dimensional breakdowns, custom date-range comparisons, exportable BI-style reports) — plans/docs/14-dashboard-frontend.md §14.4 explicitly places "analytics dashboard depth (advanced trend/drill-down views)" in Phase 2 (V2). This screen is the MVP tile-row + basic trend chart + basic aging histogram + basic gauge, not a full BI tool.
- Do not build SSE/live-push for dashboard tiles — polling only, consistent with MVP; this screen isn't named in frontend task 10's SSE retrofit scope, so don't add sockets here preemptively.
- Do not build the connector marketplace browsing UI or refined ABAC-aware dashboard filtering — both V2 per §14.4 Phase 2.
- Do not invent chart color palettes ad hoc without consulting the `dataviz` skill first — this is a hard requirement, not a style suggestion, given §14.3's "one color/typography system" mandate.

## Definition of Done
- Against real/mocked reporting data: all four tiles, the aging histogram, the SLA gauge, and the account-pair health table render correctly with realistic fixture values.
- Date-range selector on the trend chart correctly re-queries and re-renders rather than client-side slicing a fixed payload.
- Empty-tenant state (zero data) renders a clear, non-broken empty view.
- "Last updated at" timestamp reflects the actual API response time, updating correctly on each poll tick.
- Charts pass a basic accessibility check (tooltips reachable/legible, not color-only differentiation) per the `dataviz` skill's guidance.
- QA issues in the single root-level `QA_REPORT.md`.

## Common Pitfalls
- Building this screen against an assumed ClickHouse/GraphQL contract because that's what §14.2's prose literally says, ignoring the Phase 0 Postgres-materialized-view reality — verify the actual MVP reporting endpoint core task 15 exposes before assuming a data shape.
- Skipping the `dataviz` skill and hand-rolling chart colors, producing a dashboard that doesn't visually match the rest of the product's design system.
- Building deep drill-down/BI-style analytics because "it's the same screen as the target design" — that depth is explicitly V2; keep this task's scope to the tile-row + basic trend/histogram/gauge set.
- Polling the dashboard at the same aggressive interval as the Case Queue — dashboard aggregates don't need 10-15s refresh; over-polling wastes backend load for no user-visible benefit at this screen's data freshness characteristics.
- Presenting materialized-view data as if it were real-time without a "last updated" indicator, misleading users about data freshness.
