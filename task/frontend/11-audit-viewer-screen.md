# Task 11: Audit/Compliance Viewer Screen (V1)

## Goal
Build the searchable, tamper-evidence-aware trail viewer over the global `AuditEvent` log: filter by actor/entity/date/event-type, a hash-chain verification status indicator so compliance staff can trust the record hasn't been silently altered, and CSV/PDF export for regulator requests. This is V1 scope, distinct from frontend task 04's per-case timeline — this screen reads the system-wide, compliance-grade `AuditEvent` table (plans/docs/03-canonical-data-model.md §4.1), not the case-scoped `CaseComment`/`CaseAuditEvent` feed.

## Prerequisites
- Frontend task 01, frontend task 02.
- Frontend task 03 (reuses `components/data-table/` for the audit event table).
- Core task 14 (audit logging) — the `AuditEvent` table, hash-chaining (`hash_chain_prev`), and whatever chain-verification mechanism/endpoint this screen's status indicator reads from.
- Core task 15 (REST API layer) — exposes the audit query/search/export endpoints; confirm pagination/filter parameter shapes against the real contract, especially for date-range + free-text/actor/entity-type combined filtering, which is a heavier query shape than the simpler list screens in frontend tasks 03/05/06.
- Core task 23 (RBAC/ABAC via OPA, V1) — this screen is realistically restricted to the Auditor/Read-Only and Tenant Admin roles (plans/docs/09-security-compliance.md §10.3); confirm whether route-level access gating (not full ABAC, per the V2 boundary already established in other tasks) is needed here.

## Scope / Deliverables
- `web/app/(dashboard)/audit/page.tsx` — the screen: filter bar (actor, entity type, entity ID, event type, date range), the audit event table, a hash-chain verification status banner, export actions (CSV, PDF).
- `web/components/audit/audit-event-table.tsx` — reuses `components/data-table/`; columns: timestamp (`occurred_at`), actor (+ actor_type), event_type, entity_type/entity_id, a summarized before/after diff, request/trace correlation id.
- `web/components/audit/audit-event-detail.tsx` — expandable row detail: full `before_state`/`after_state` JSON diff (readable diff view, not two raw JSON blobs side by side with no visual diffing), IP/geo, actor auth method, request correlation id (linking conceptually to the originating API call/trace per plans/docs/09-security-compliance.md §10.1 — a literal trace-viewer integration is not required, just displaying the `request_id` value is sufficient for MVP-of-this-screen).
- `web/components/audit/hash-chain-status.tsx` — the verification indicator: shows whether the currently-loaded event range's hash chain has been verified intact (per plans/docs/09-security-compliance.md §10.1's periodic verification job) — this reads a verification *status* from the backend (last verification run time + pass/fail + any flagged broken-link position), it does not recompute/verify the hash chain client-side.
- `web/components/audit/export-actions.tsx` — CSV export (straightforward tabular download of the current filtered result set) and PDF export (formatted report, likely generated server-side and downloaded, given PDF generation is not a sensible client-side responsibility for a compliance document) — confirm whether export is synchronous (direct download) or async (a generated-report job the UI polls for completion), matching whatever core task 15 actually implements, per the "long-running-op pattern for async actions" convention noted in plans/docs/07-api-extensibility.md §8.1.
- `web/lib/api/endpoints/audit.ts` — `searchAuditEvents(filters, pageToken)`, `getHashChainVerificationStatus()`, `exportAuditEvents(filters, format)`.

## Design Reference
- plans/docs/14-dashboard-frontend.md §14.2 screen 8 — exact scope: searchable `AuditEvent` trail (filter by actor/entity/date/event-type), hash-chain verification status indicator, export-to-CSV/PDF for regulator requests.
- plans/docs/09-security-compliance.md §10.1 — the hash-chain design this status indicator reflects: "each event's `hash_chain_prev` links to previous event's `SHA-256(payload + prev_hash)` — retroactive modification breaks the chain, detectable via periodic verification job." The verification job itself is backend logic (core task 14); this screen surfaces its output.
- plans/docs/03-canonical-data-model.md §4.1 — `AuditEvent` fields verbatim: `id (ULID, time-sortable)`, `tenant_id`, `actor_id`, `actor_type`, `event_type`, `entity_type`, `entity_id`, `before_state (jsonb)`, `after_state (jsonb)`, `ip_address`, `request_id`, `occurred_at`, `hash_chain_prev`.
- plans/docs/09-security-compliance.md §10.2 — SOC2/PCI-DSS context for *why* this screen exists (regulator requests, quarterly RBAC access-review reports) — informs the export feature's purpose (this is a compliance-audit tool, not a general activity log for casual browsing), but this task does not need to build the access-review report generation itself unless explicitly asked; the export action here is the general-purpose CSV/PDF of filtered results.

## Implementation Notes
- Filtering: date-range is likely the most commonly combined filter with actor/entity/event-type — default to a sensible recent range (e.g. last 30 days) rather than an unbounded query on initial load, given `AuditEvent` is a system-wide, potentially very large table; make the "no date range" (full history) case an explicit, deliberate user action, not the default.
- Because `AuditEvent.id` is a ULID (time-sortable), default sort is by `id`/`occurred_at` descending (most recent first) — don't re-derive sort order from a separate timestamp field when the ID itself is already time-ordered.
- Hash-chain status banner states: "Verified intact as of [last verification run timestamp]" (healthy), "Verification pending" (no run yet or stale), "Chain integrity issue detected at [event reference]" (failure — render this prominently, not as a subtle warning, given the compliance stakes) — confirm the actual status API shape before assuming these three states are exhaustive.
- Before/after diff rendering: a simple key-by-key comparison highlighting changed fields (added/removed/changed) is sufficient — do not build a generic deep-JSON-diff library integration if a straightforward top-level-key diff covers the realistic `AuditEvent.payload` shapes; escalate to a proper diff library only if nested-object diffing proves genuinely necessary once real payload shapes are seen.
- Export: if async (job-based), show a "preparing export..." state with polling for completion and then a download link/auto-download, consistent with the "long-running-op pattern for async actions" convention named in plans/docs/07-api-extensibility.md §8.1 (the same pattern likely used elsewhere in the API, e.g. "trigger re-match run") — don't invent a bespoke async-UI pattern different from whatever convention the rest of the app's async actions use, if one already exists by the time this task is built.

## Non-Goals / Guardrails
- Do not build the hash-chain computation/verification logic itself (SHA-256 chaining, periodic verification job) — that's core task 14's backend scope entirely; this screen only displays the verification job's output status.
- Do not build the quarterly RBAC access-review report generation feature mentioned in plans/docs/09-security-compliance.md §10.2 unless it's explicitly folded into this task later — treat it as a distinct, not-yet-scoped feature; this task's export is the general filtered-CSV/PDF export named in §14.2 screen 8, not that specific compliance report.
- Do not conflate this screen with frontend task 04's per-case timeline — do not attempt to merge or share the timeline-rendering component between the two; the data source (global `AuditEvent` vs. per-case `CaseComment`/`CaseAuditEvent`), scale, and interaction model (search/export vs. inline case-work) are different enough to warrant separate components even though both display chronological event data.
- Do not build a live/real-time-updating audit feed (SSE) — this screen is not named in frontend task 10's SSE retrofit scope; a compliance search/export tool does not need live-push updates.
- Do not build WORM archive browsing (querying the S3/Parquet cold-archive tier directly per plans/docs/08-storage-architecture.md §9.4) — this screen reads the queryable hot-tier `AuditEvent` data via the API; archived/cold-tier record access (if ever needed) is a separate, unscoped capability.

## Definition of Done
- Filtering by actor, entity type, event type, and date range (individually and combined) returns correctly scoped results against real/mocked fixture data.
- Before/after diff view correctly highlights changed fields for a realistic fixture payload (e.g. a `Break` status transition's before/after state).
- Hash-chain status banner correctly reflects a healthy state and, when simulated against a fixture "broken chain" status response, correctly renders the failure state prominently.
- CSV export downloads a correctly-scoped (filtered) result set; PDF export either downloads directly or completes the async job-and-poll flow, whichever the real API implements — verified against the actual contract, not assumed.
- QA issues in the single root-level `QA_REPORT.md`.

## Common Pitfalls
- Defaulting to an unbounded (all-time, all-tenant-history) query on initial page load — a real performance and usability problem against a large compliance log; default to a recent window.
- Building a full hash-verification recomputation in the browser "to double-check the backend" — this is explicitly backend-owned logic; the frontend trusts and displays the backend's verification job output.
- Merging this screen's audit table component with frontend task 04's case timeline component for "code reuse" — they read different data sources at different scales for different purposes; forcing a shared component creates awkward prop-drilling and conditional logic instead of genuine reuse.
- Inventing a bespoke async-export polling UI pattern if the app already has an established one elsewhere (per the "long-running-op" convention) — check for and reuse an existing pattern rather than adding a second one.
- Treating the `request_id` field as merely decorative text instead of a genuinely useful correlation reference — even without a full trace-viewer integration, make it copyable/visible enough to be useful for a support engineer cross-referencing it against logs/traces.
