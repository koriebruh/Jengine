> Part of the Jengine Reconciliation Engine design doc set. Index: [README.md](README.md) · Prev: [13-implementation-notes.md](13-implementation-notes.md)

# 14 — Dashboard / Frontend Architecture

Note on sequencing: frontend is built **last** in each phase (backend/API/data model must stabilize first — the API is the contract, the UI is a client of it). But it's planned now, not after, because screen requirements shape API design decisions (pagination shape, webhook event granularity, what needs a streaming/WS channel vs plain REST) — deciding this late forces API rework.

## 14.1 Tech Stack & Justification

| Layer | Choice | Why |
|---|---|---|
| Framework | **Next.js (App Router) + TypeScript** | SSR for the initial dashboard load (fast perceived load on data-heavy screens), React Server Components for read-only report pages, file-based routing scales cleanly across the many screens below. |
| Data fetching/cache | **TanStack Query** | Server-state caching, polling/refetch-on-focus for near-real-time widgets, optimistic updates for case actions (assign/comment/approve) without hand-rolled cache invalidation. |
| Tables/grids | **TanStack Table** | Headless — needed for the break/case queue and match-review queue, which require virtualized large lists, column sort/filter, row-selection for bulk actions. |
| UI components | **shadcn/ui + Tailwind** | Unstyled Radix primitives + Tailwind gives full control over visual identity (avoids the generic "looks like every other admin panel" trap ReconArt's UI reportedly suffers from) while keeping accessibility (Radix) for free. |
| Charts | **visx or Recharts**, following the `dataviz` design-system approach (categorical/sequential palette consistency, accessible tooltips/legends) | Match-rate trends, break-aging histograms, SLA-compliance gauges — all fed from the ClickHouse-backed reporting API ([08-storage-architecture.md](08-storage-architecture.md) §9.2). |
| Real-time updates | **Server-Sent Events (SSE)** per tenant channel, fed from `case.events` / `matching.results` Kafka topics via a thin WS/SSE gateway | Case queue and match-review queue update live without polling; SSE chosen over full WebSocket for simplicity (one-directional server→client push is all the UI needs; client actions go back over normal REST/gRPC calls, not over the socket). |
| Auth | Same OIDC/SSO session as the API layer ([07-api-extensibility.md](07-api-extensibility.md) §8.1) — NextAuth.js or a thin custom OIDC client, session cookie forwarded as bearer token to Connect-RPC/REST calls | One identity provider integration serves both API and UI — no separate frontend auth system to maintain. |
| State (client-local UI state) | React Context + `useState`/`useReducer` for local view state; **no global client-state library** (Redux/Zustand) needed — TanStack Query already owns server state, which is the vast majority of what this app renders | Avoids the classic over-engineering trap of adding a global store when almost all state is server-derived. |

## 14.2 Key Screens

1. **Reconciliation Overview Dashboard** (tenant home) — match rate %, auto-match %, break volume/aging trend, SLA compliance gauge, per-account-pair health tiles. Backed by ClickHouse materialized views (`mv_match_rate_by_rule`, `mv_breaks_daily_aging`, `mv_sla_compliance`), refreshed near-real-time via CDC.
2. **Case/Break Queue** — filterable, sortable, virtualized table of open breaks (by account, priority, root-cause, assignee, SLA countdown). Row-selection → bulk assign/comment/resolve. Live-updates via SSE when new breaks open or existing ones change status.
3. **Case Detail View** — break metadata, linked unmatched/partially-matched transactions, comment/audit timeline (interleaved `CaseComment` + `CaseAuditEvent`), assign/escalate/approve actions, root-cause tagging, attachment upload.
4. **Match Review Queue** ("suggested matches" inbox) — one candidate per row, confidence score with per-field similarity breakdown (transparency differentiator, see [12-competitive-differentiation.md](12-competitive-differentiation.md)), one-click confirm/reject, bulk-confirm for high-confidence batches.
5. **Rule Builder** — no-code visual builder compiling to the DSL from [04-matching-engine.md](04-matching-engine.md) §5.1 (scope picker, blocking-key config, scoring-weight sliders, threshold sliders), plus the **backtesting sandbox** panel (replay a historical date range, show projected auto-match rate / false-positive risk / break-volume delta before activating — §5.4). Rule activation triggers the maker-checker approval flow, so this screen shows pending-approval state too.
6. **Connector/Ingestion Monitor** — per-connector status (last run, records ingested, error rate), quarantine-queue viewer with inline remediation, DLQ browser + manual redrive action ([02-data-ingestion.md](02-data-ingestion.md) §3.3, [10-observability-reliability.md](10-observability-reliability.md) §11.3).
7. **Tenant Admin** — user/role management (RBAC assignment), webhook subscription config + delivery-log/redrive UI ([07-api-extensibility.md](07-api-extensibility.md) §8.2), API key management, isolation-tier/quota display (read-only for Standard tier, editable for platform ops).
8. **Audit/Compliance Viewer** — searchable `AuditEvent` trail (filter by actor/entity/date/event-type), hash-chain verification status indicator, export-to-CSV/PDF for regulator requests.

## 14.3 Component Architecture

- `components/data-table/` — one virtualized-table primitive (TanStack Table wrapper) reused across Case Queue, Match Review Queue, Connector Monitor, Audit Viewer — avoids four bespoke table implementations.
- `components/charts/` — shared chart primitives (trend line, aging histogram, gauge) following one color/typography system so the dashboard reads as one product, not a bag of ad-hoc widgets.
- `components/case/` — case-domain components (timeline, status badge, SLA countdown chip) shared between Case Queue and Case Detail.
- `lib/api/` — generated TypeScript client from the same `.proto`/Buf definitions the backend uses ([07-api-extensibility.md](07-api-extensibility.md) §8.1, via `buf generate` with a TS/Connect-Web plugin) — frontend and backend share one contract, no hand-maintained API types.

## 14.4 Where This Lands in the Roadmap

- **Phase 0 (MVP)**: minimal internal UI only — a thin Case Queue + Case Detail + basic connector status page, enough for design-partner analysts to work breaks without needing the API directly. No rule-builder UI yet (rules authored as raw YAML/JSON via API/CLI at MVP). No SSE — polling is fine at MVP volume.
- **Phase 1 (V1)**: full screen set from §14.2 — rule builder with backtesting sandbox, tenant admin, webhook config UI, SSE-based live updates. This is what ships as the sellable product surface.
- **Phase 2 (V2)**: analytics dashboard depth (advanced trend/drill-down views), connector marketplace browsing UI, refined ABAC-aware UI (hide actions a user's OPA policy would reject, rather than showing then erroring).

---
Next: [15-end-to-end-flows.md](15-end-to-end-flows.md)
