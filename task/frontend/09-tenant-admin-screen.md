# Task 09: Tenant Admin Screen (V1)

## Goal
Build the tenant-facing administration surface: user/role management (RBAC assignment), webhook subscription configuration with a delivery-log/redrive UI, API key management, and isolation-tier/quota display. This is V1 scope — the webhook system, full RBAC/ABAC, and full multi-tenancy isolation tiers it manages are all V1 core capabilities (core tasks 21, 23, 24). Do not start before MVP frontend tasks (01–07) are complete.

## Prerequisites
- Frontend task 01, frontend task 02.
- Frontend task 03 (reuses `components/data-table/` for user lists, webhook delivery logs, API key lists).
- Core task 04 (tenancy context and routing) — tenant identity/config this screen displays and edits.
- Core task 21 (webhook system, V1) — the webhook subscription CRUD + delivery-log + redrive endpoints this screen's webhook panel is built against.
- Core task 23 (security hardening: RBAC/ABAC via OPA, V1) — the role catalog (Tenant Admin, Recon Manager, Analyst, Approver, Auditor/Read-Only, API Integration Role per plans/docs/09-security-compliance.md §10.3) this screen's user/role management panel assigns.
- Core task 24 (full multi-tenancy isolation tiers, V1) — the isolation-tier/quota data this screen displays.
- Core task 15 (REST API layer) must expose whatever API-key issuance/management endpoints exist — confirm whether API key management is part of core task 15 (MVP, since scoped API keys are mentioned in plans/docs/07-api-extensibility.md §8.1 as a base auth mechanism) or hardened/extended in core task 23 (V1) before assuming the endpoint contract.

## Scope / Deliverables
- `web/app/(dashboard)/admin/page.tsx` — admin landing/overview (tabs or sub-nav to the four sections below).
- `web/app/(dashboard)/admin/users/page.tsx` — user list (name, email, assigned role(s), status, last login), invite-user action, role-assignment editor per user.
- `web/app/(dashboard)/admin/webhooks/page.tsx` — webhook subscription list (endpoint URL, subscribed event types, filter rules e.g. "only breaks above $50k", status), create/edit subscription form, and a delivery-log view (per-delivery status, response code, timestamp, retry count) with a manual redrive action per failed delivery.
- `web/app/(dashboard)/admin/api-keys/page.tsx` — API key list (name, scope, created date, last used, masked key value), create/revoke actions.
- `web/app/(dashboard)/admin/quotas/page.tsx` — isolation-tier display (Standard/Isolated/Dedicated, read-only for Standard tier tenants, editable only for platform-ops users) and quota usage (ingestion rate limits, matching compute, current usage vs. limit).
- `web/components/admin/role-assignment-editor.tsx` — role picker per user, scoped to the RBAC role catalog.
- `web/components/admin/webhook-subscription-form.tsx` — event-type multi-select (from the event catalog: `transaction.ingested`, `match.found`, `match.auto_confirmed`, `break.created`, `break.assigned`, `break.sla_warning`, `break.sla_breached`, `break.resolved`, `case.approval_requested`, `rule.activated`, etc.), endpoint URL + secret display (HMAC signing secret shown once at creation, masked thereafter), filter-rule builder (simple field/operator/value form, e.g. `amount_at_risk > 50000`).
- `web/components/admin/webhook-delivery-log.tsx` — reuses `components/data-table/`, per-row expandable payload/response detail, redrive button per failed row.
- `web/components/admin/api-key-manager.tsx` — key list + create dialog (name, scope selection) + one-time key-value reveal on creation (never shown again after) + revoke action.
- `web/lib/api/endpoints/tenant-admin.ts` — `listUsers()`, `assignRole(userId, role)`, `listWebhookSubscriptions()`, `createWebhookSubscription(payload)`, `updateWebhookSubscription(id, payload)`, `listWebhookDeliveries(subscriptionId, filters)`, `redriveWebhookDelivery(deliveryId)`, `listApiKeys()`, `createApiKey(payload)`, `revokeApiKey(id)`, `getIsolationTierAndQuota()`.

## Design Reference
- plans/docs/14-dashboard-frontend.md §14.2 screen 7 — exact scope: user/role management, webhook subscription config + delivery-log/redrive UI, API key management, isolation-tier/quota display (read-only Standard, editable platform ops).
- plans/docs/07-api-extensibility.md §8.2 — webhook event catalog, HMAC-SHA256 signing (tenant-specific secret), delivery status tracking, DLQ + manual redrive UI **"visible to tenant admin (transparency differentiator vs black-box legacy integration)"** — this delivery-log/redrive visibility is itself a named differentiator, treat it with the same seriousness as frontend task 05's confidence breakdown, not as an afterthought tab.
- plans/docs/09-security-compliance.md §10.3 — the RBAC role catalog and the ABAC/OPA layer on top (Rego-policy-driven; this screen manages RBAC role assignment only, it does not need to build an OPA policy editor — ABAC policy authoring isn't named in §14.2 screen 7's scope).
- plans/docs/01-multi-tenancy.md §2.1/§2.4 — isolation tier table and quota/noisy-neighbor mitigation this screen's quota panel visualizes (per-tenant rate limits, soft-throttle 429 behavior — this screen shows current usage, it doesn't implement the throttling itself).

## Implementation Notes
- Role assignment: a user can plausibly hold more than one role — confirm the actual data model (single role vs. multi-role per user) against core task 23's implementation before building a single-select vs. multi-select control.
- Webhook secret handling: the HMAC signing secret must be treated as write-once-display — show it in a copyable box immediately after creation with a clear "this won't be shown again, copy it now" warning, then never re-render the actual secret value again (masked/redacted on subsequent views), mirroring the same pattern for API key values.
- Delivery log filter rule builder: keep this simple (field/operator/value rows, AND-combined) — do not build a full expression-language editor; the design doc's own example ("only breaks above $50k") is a single simple comparison, not a complex boolean-logic system.
- Redrive action (webhook delivery): a single-delivery retry button, similar in spirit to frontend task 06's DLQ redrive but a functionally distinct backend operation (webhook re-delivery vs. pipeline record reprocessing) — do not reuse frontend task 06's DLQ components directly; build a delivery-log-specific component even if visually similar, since the data shapes and retry semantics differ.
- Isolation-tier editability: gate the "editable" quota/tier controls behind whatever platform-ops role check the session exposes (a session role check, not a full ABAC policy evaluation — that's V2 per §14.4) — a Standard-tier tenant admin should see the tier/quota panel as read-only informational display, not be able to submit tier-change requests from this screen.
- Quota usage visualization: simple progress-bar/gauge style tiles (current usage vs. limit for ingestion rate, matching compute) — reuse `components/charts/`'s gauge primitive from frontend task 07 if it fits, rather than building a new one.

## Non-Goals / Guardrails
- Do not build an OPA/Rego policy editor — ABAC policy authoring isn't in §14.2 screen 7's scope; this screen only assigns RBAC roles, not fine-grained attribute policies.
- Do not build tenant onboarding flows (creating a brand-new tenant, initial account/connector setup) — that's the Tenant Onboarding flow (plans/docs/15-end-to-end-flows.md §15.4), which spans platform-ops tooling and multiple screens already covered elsewhere (Connector Monitor for connector setup status, this screen for role/webhook/API-key config after the tenant already exists) — do not build a "create tenant" wizard here.
- Do not implement the actual webhook dispatch/retry/HMAC-signing logic — that's core task 21's backend scope; this screen only displays delivery status and triggers redrive via API call.
- Do not build quota *editing* for non-platform-ops users — Standard tier must remain read-only per the explicit design requirement.
- Do not reuse frontend task 06's DLQ browser component wholesale for the webhook delivery log — build a delivery-log-specific component; the underlying data/actions are different even if visually similar.

## Definition of Done
- User list renders, role assignment succeeds and persists, verified against a real/mocked RBAC role catalog matching plans/docs/09-security-compliance.md §10.3's list.
- Webhook subscription can be created (event types + filter + endpoint), its HMAC secret is shown exactly once, and a subsequent view shows it masked.
- Delivery log shows realistic fixture deliveries (success/failure/retrying states) and a failed delivery can be redriven, with the row reflecting the new attempt.
- API key creation shows the key value once, subsequent views show it masked; revoke works and revoked keys are visually distinguished from active ones.
- Isolation-tier/quota panel renders correctly for both a Standard-tier session (read-only) and a platform-ops session (editable) — verify both role states render distinctly.
- QA issues in the single root-level `QA_REPORT.md`.

## Common Pitfalls
- Re-displaying a webhook secret or API key value after its initial creation view — a real security/compliance issue in this domain, not just a UX nit.
- Building a full boolean-expression filter-rule editor when the design only calls for simple field/operator/value comparisons.
- Allowing quota/tier edits to render as available (even if disabled-looking) for Standard-tier sessions in a way that could be bypassed client-side — the backend must also enforce this, but the frontend shouldn't even present an edit affordance that creates a false impression of tenant-level control.
- Conflating RBAC role assignment with ABAC policy configuration — this screen does the former only; building an OPA policy UI here is scope creep into functionality not named for this screen.
- Building a "create new tenant" flow here — tenant creation is a platform-ops/onboarding concern outside this screen's scope.
