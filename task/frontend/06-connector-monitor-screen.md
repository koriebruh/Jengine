# Task 06: Connector / Ingestion Monitor Screen

## Goal
Build the operational visibility screen for the ingestion layer: per-connector run status, a quarantine-queue viewer with inline remediation for records that failed validation, and a DLQ browser with manual redrive for records that failed unrecoverably further downstream. This is the third of the three screens plans/docs/14-dashboard-frontend.md §14.4 names as MVP-critical, and it is the UI for the failure/redrive flow in plans/docs/15-end-to-end-flows.md §15.5 — build it to actually support that flow's two distinct remediation paths (quarantine fix-and-reprocess vs. DLQ redrive), not as a generic "connector list with a status column."

## Prerequisites
- Frontend task 01, frontend task 02.
- Frontend task 03 — reuses `components/data-table/` for the quarantine and DLQ lists.
- Core task 06 (ingestion connector framework) and core task 07 (ingestion MVP connectors: CSV/SFTP/MT940) — define `Connector`/`ConnectorConfig` status fields this screen reads.
- Core task 09 (idempotency and validation) — owns the quarantine queue (schema-validation/business-validation failures per plans/docs/02-data-ingestion.md §3.3) this screen's remediation panel is built against; this is the primary MVP-real data source for this screen.
- Core task 15 (REST API layer, MVP) — exposes connector status + quarantine endpoints.

## ⚠️ Scope Note on the DLQ Browser
plans/docs/14-dashboard-frontend.md §14.4 describes MVP's frontend connector screen minimally as a "basic connector status page," with the fuller quarantine/DLQ/redrive feature set implied for the Phase 1 (V1) "full screen set." However, the master frontend task list (`task/README.md`) places this task (frontend 06) in the MVP phase, and this task's own brief explicitly asks for quarantine viewer + DLQ browser + redrive — following flow 15.5. Resolve this by building **both** pieces, but treating them as two independently-shippable panels with different confidence levels:
1. **Quarantine queue viewer + inline remediation** — build this as the solid, fully-functional MVP deliverable. It is backed by core task 09, which is unambiguously MVP scope (plans/docs/02-data-ingestion.md §3.3's quarantine queue exists from day one, independent of Kafka/streaming).
2. **DLQ browser + manual redrive** — the `dlq.<stage>` Kafka topics described in plans/docs/06-streaming-architecture.md §7.1 and plans/docs/15-end-to-end-flows.md §15.5 are part of the Kafka-based pipeline; full Kafka-based streaming ingestion is core task 18, **V1 scope** per plans/docs/11-scalability-roadmap.md §12.2. It is not yet confirmed whether core task 09/15 (MVP) expose *any* DLQ-shaped endpoint before core task 18 lands. Build the DLQ browser UI against the API contract described in flow 15.5 (record payload + error context + redrive action), but **feature-detect or feature-flag it** — if no DLQ endpoint exists yet when this task is implemented, ship the quarantine panel alone and leave the DLQ panel visibly present but in an honest "not yet available" state, not hidden and not faked. Confirm the real endpoint contract with whatever core task actually lands DLQ read/redrive (core task 09 if it's pulled forward, or core task 18 if it stays V1-gated) and wire it for real at that point.

## Scope / Deliverables
- `web/app/(dashboard)/connectors/page.tsx` — the screen: connector status grid/list (one card or row per configured connector: type, last run time, records ingested, error rate, next scheduled run), a Quarantine tab, a DLQ tab.
- `web/components/connector/connector-status-card.tsx` — per-connector summary (status indicator: healthy/degraded/failing, last run timestamp, ingested-record count, error rate over recent runs).
- `web/components/connector/quarantine-panel.tsx` — table (via `components/data-table/`) of quarantined records: raw payload, failure reason, connector/source, timestamp; row action "remediate inline" (edit the offending field(s) per the tenant's field-mapping DSL context and resubmit) and "dismiss" (mark as reviewed/ignored, distinct from resubmit).
- `web/components/connector/dlq-panel.tsx` — table of DLQ'd records: stage (`connector parse` / `mapping transform` / `matching write`, per plans/docs/15-end-to-end-flows.md §15.5 step 1), full error context, payload preview, single-record "redrive" action and a batch "redrive date range" action (for the "systemic failure → replay" case in §15.5 step 3).
- `web/lib/api/endpoints/connectors.ts` — `listConnectors()`, `getConnectorStatus(id)`, `listQuarantinedRecords(filters)`, `remediateQuarantinedRecord(id, correctedFields)`, `dismissQuarantinedRecord(id)`, `listDlqRecords(filters)`, `redriveDlqRecord(id)`, `redriveDlqRange(stage, dateRange)`.

## Design Reference
- plans/docs/14-dashboard-frontend.md §14.2 screen 6 — per-connector status, quarantine-queue viewer with inline remediation, DLQ browser + manual redrive.
- plans/docs/02-data-ingestion.md §3.3 — quarantine queue semantics: "Failures land in quarantine queue with raw payload + reason, surfaced for manual remediation — never silently drop financial data." This is the framing for the quarantine panel's copy/empty-states: quarantine is not an error log, it's a required-attention work queue (similar in spirit to the Case Queue).
- plans/docs/15-end-to-end-flows.md §15.5 — the two remediation paths this screen must support: (1) single-record DLQ inspect → fix root cause → manual redrive through the pipeline entrypoint; (2) systemic failure → replay a whole affected date range (idempotency-key-guarded, safe to reprocess per the doc).
- plans/docs/10-observability-reliability.md §11.3 — "DLQ + manual redrive tooling per stage with full error context" — the error context shown must be genuinely useful for root-cause diagnosis (stack trace / validation rule that failed / record field values), not just a generic "failed" status.

## Implementation Notes
- Connector status polling: same MVP polling convention as frontend task 03 (a `refetchInterval`, not SSE) — connector health monitoring benefits from near-real-time updates but MVP scope is explicitly polling-based per §14.4.
- Quarantine inline remediation: the "corrected fields" form should be driven by the tenant's field-mapping spec (plans/docs/02-data-ingestion.md §3.2) if the API exposes it, so the remediation form shows the actual mapped target fields (`transaction.amount`, `transaction.currency`, etc.) rather than raw source column names — this makes remediation genuinely usable for a non-technical ops user, matching the design's "lets non-technical ops onboard new source formats without code" intent. If the mapping spec isn't available via API yet, fall back to a raw-JSON edit box for the payload but flag this as a UX gap to revisit, not a permanent design.
- DLQ redrive: single-record redrive is a simple button + confirmation; date-range redrive (the "systemic failure" path) should require an explicit confirmation dialog stating what will be reprocessed (stage + date range + estimated record count if the API returns one) since it's a heavier, more consequential action — don't make it a casual one-click button styled identically to single-record redrive.
- Empty/loading/error states: distinguish "no quarantined records right now" (good state, calm styling) from "quarantine data failed to load" (error state) — same principle as frontend task 03's case queue.
- Record payload display: pretty-print JSON with syntax highlighting is a reasonable nice-to-have but not required; a readable formatted view (not a single unstyled blob of raw JSON) is the actual bar.

## Non-Goals / Guardrails
- Do not build connector *configuration* (creating/editing a `ConnectorConfig`, credential setup) — that's part of Tenant Onboarding (plans/docs/15-end-to-end-flows.md §15.4) and Tenant Admin (frontend task 09, V1). This screen is read/remediate-only over already-configured connectors.
- Do not implement the Connector SDK certification/marketplace browsing UI — that's explicitly V2 per plans/docs/11-scalability-roadmap.md §12.2 Phase 2 and plans/docs/07-api-extensibility.md §8.3.
- Do not fabricate DLQ data or silently no-op the redrive button if the backend endpoint doesn't exist yet — per the Scope Note above, show an honest "not yet available" state instead.
- Do not build SSE/live-push for connector status — polling only, consistent with the rest of MVP screens; not named in frontend task 10's retrofit scope either, so don't preemptively wire sockets here.
- Do not implement the actual replay/reprocessing execution logic (idempotency-key-guarded pipeline replay) — that is backend logic (core task 09 and/or core task 18); this screen only triggers it via an API call and shows the result/status.

## Definition of Done
- Connector status cards render correctly against fixture data across healthy/degraded/failing states.
- Quarantine panel: a quarantined record can be remediated inline (edited and resubmitted) and disappears from the quarantine list on success; a dismissed record is distinguishable from a remediated one (different resulting state, not just removed from view identically).
- DLQ panel: if a real DLQ endpoint exists at implementation time, single-record and date-range redrive both work end-to-end and are verified against actual backend behavior (not assumed). If no such endpoint exists yet, the panel visibly and honestly communicates that, and this is explicitly noted as a follow-up in `QA_REPORT.md` rather than silently shipped as if complete.
- Manual verification: simulate a quarantine failure via fixture data, inline-remediate it, confirm it clears from the queue.
- QA issues in the single root-level `QA_REPORT.md`.

## Common Pitfalls
- Building only a "basic connector status page" and skipping quarantine/DLQ entirely because §14.4's phase-placement language undersells this screen's MVP scope relative to this task's explicit brief — the master task list and this task's brief take precedence; build the fuller scope, with the DLQ honesty caveat above.
- Faking DLQ data with static/mock content and shipping it as if it were real, live backend data — actively misleading for an ops screen whose entire purpose is trustworthy failure visibility.
- Treating "dismiss" and "remediate" as the same action in the UI — they have different meanings (dismiss = acknowledged, no fix needed / accepted as-is; remediate = corrected and resubmitted into the pipeline) and must be distinct, auditable actions.
- Making date-range redrive as low-friction as single-record redrive — it's a heavier operation (potential mass reprocessing) and needs its own confirmation step.
- Hardcoding raw source column names in the remediation form instead of using the tenant's actual field-mapping spec, if available — undermines usability for the non-technical ops persona this feature targets.
