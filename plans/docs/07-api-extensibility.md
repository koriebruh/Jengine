> Part of the Jengine Reconciliation Engine design doc set. Index: [README.md](README.md) · Prev: [06-streaming-architecture.md](06-streaming-architecture.md)

# 07 — API & Extensibility Layer

## 8.1 REST/gRPC Design Principles
- Contract-first: all services in `.proto` (Buf-managed `proto/`), served via Connect-RPC (gRPC + gRPC-Web + REST/JSON from one implementation).
- Resource-oriented REST surface (`/v1/tenants/{id}/accounts`, `/v1/tenants/{id}/breaks/{id}/comments`), AIP-style conventions (pagination via `page_token`, structured filtering, long-running-op pattern for async actions like "trigger re-match run").
- Versioning: URL-path (`/v1/`, `/v2/`), min 12-month deprecation sunset (bank integration teams move slowly).
- Idempotency: `Idempotency-Key` header on all mutating endpoints — critical to prevent double-submission on client retry.
- Auth: OAuth2 client-credentials (M2M), OIDC/SAML SSO (Okta/Azure AD/Ping — common bank requirement), scoped API keys, all mapped to RBAC/ABAC (see [09-security-compliance.md](09-security-compliance.md) §10.3).

## 8.2 Webhook System
- Event catalog: `transaction.ingested`, `match.found`, `match.auto_confirmed`, `break.created`, `break.assigned`, `break.sla_warning`, `break.sla_breached`, `break.resolved`, `case.approval_requested`, `rule.activated`, etc.
- Delivery: transactional outbox → `webhook.outbox` → Webhook Dispatcher service — exponential backoff retry, HMAC-SHA256 signing (tenant-specific secret), delivery status tracking, DLQ + manual redrive UI **visible to tenant admin** (transparency differentiator vs black-box legacy integration).
- Per-event-type subscriptions with filtering (e.g. "only breaks above $50k").

## 8.3 Plugin/Connector SDK
- Published Connector SDK (Go module + WASM host interface): `SourceConnector` interface, scaffold CLI (`jengine-connector new`), local test harness (mock pipeline, sample data, dry-run mapping) before certification submission.
- Connector marketplace/certification process (manual review + automated security/sandbox scan) — targets ReconArt's closed-integration criticism directly; ecosystem play analogous to Fivetran/Segment connector ecosystems.

## 8.4 GraphQL for Reporting
- Separate GraphQL gateway (`gqlgen`) in front of ClickHouse, strictly read-only/reporting-scoped — flexible ad-hoc nested queries for analysts/BI tools (Tableau/PowerBI). All mutations remain REST/gRPC — avoids general-purpose GraphQL mutation-surface complexity.

---
Next: [08-storage-architecture.md](08-storage-architecture.md)
