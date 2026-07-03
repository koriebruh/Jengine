# Task 21: Webhook System (Outbound)

## Goal
Build the outbound webhook notification system: the full event catalog, delivery from the transactional outbox through a dedicated Webhook Dispatcher service, HMAC-SHA256 signing, exponential-backoff retry, a dead-letter queue, and the backend for a tenant-visible redrive capability. This directly answers ReconArt's "closed/opaque integration" weakness — tenants must be able to see and act on their own delivery failures, not file a support ticket. This task also owns the SSE/WS real-time gateway (see the dedicated subsection below) that `plans/task/frontend/10-realtime-sse-integration.md` depends on, since it is a second, lightweight consumer of the same event-catalog infrastructure — no other core task claims this piece, and splitting it into a separate task would duplicate the event-catalog/topic-consumption plumbing built here.

## Prerequisites
- Core task 18 (outbox pattern, `webhook.outbox` topic already defined there).
- Core task 15 (MVP REST API — being extended here, see the Connect-RPC note below).
- Core task 14 (audit logging — webhook subscription changes and deliveries are audit-relevant).
- Core task 20 (several catalog events — `break.created`, `break.assigned`, `sla_warning`/`sla_breached`, `case.approval_requested` — originate from Temporal Activities added there; this task only owns their delivery, not their emission).
- Core task 19 (`match.auto_confirmed` originates from the streaming/batch reconciliation job; also the source of the `matching.results.<shard>` topic the SSE gateway subsection below consumes).
- Core task 20 (source of the `case.events.<shard>` topic the SSE gateway subsection below consumes).

## Scope / Deliverables
- `cmd/webhook-dispatcher/main.go` (already named in the repo layout — this task is what actually builds it).
- `internal/notify/` — event catalog constants, subscription matching/filtering, signing, retry scheduling, delivery bookkeeping.
- `migrations/00xx_webhook_subscription_delivery.sql` — `webhook_subscription`, `webhook_delivery` tables.
- `proto/jengine/v1/webhook.proto` — `WebhookService` (subscription CRUD, delivery log listing, redrive).
- Connect-RPC server mount inside `cmd/coreapi` (or `cmd/api-gateway`, wherever task 15's HTTP surface lives) serving `WebhookService` alongside the existing services.
- SSE gateway endpoint (`/v1/tenants/{tenant_id}/events/stream` + the stream-token mint RPC) — see the dedicated Implementation Notes subsection below.

## Design Reference
- `plans/docs/07-api-extensibility.md` §8.2 (event catalog, delivery mechanics, HMAC signing, DLQ + redrive), §8.1 (Connect-RPC contract-first design — see the note below on how this task applies it).
- `plans/docs/06-streaming-architecture.md` §7.1 (`webhook.outbox` topic, 7-day retention), §7.3 (outbox pattern — reused unchanged from task 18).
- Do not repeat the rationale here; open the referenced sections for it.

## Implementation Notes

### API contract — reconciled with task 15 (correction)
Core task 15 (MVP REST API) already resolved this: MVP endpoints are built contract-first via `.proto` + Connect-RPC from day one (Connect-RPC serves plain JSON-over-HTTP from the same definition that also serves gRPC/gRPC-Web — there is no separate hand-rolled `net/http`/chi REST implementation to reconcile with). This task's webhook subscription CRUD, delivery-log listing, and redrive endpoints are simply **new proto services added beside the ones task 15 already defined**, served through the same Connect-RPC mount inside `cmd/coreapi` — not the first use of the contract-first pattern, just the next set of services using it.

The scoping call that still matters: **do not** use this task as an excuse to touch or refactor task 15's existing `AccountService`/`TransactionService`/`MatchRuleService`/`MatchReviewService`/`BreakService` definitions — add `WebhookService` as an independent proto file and service registration, don't modify the others.

### Event catalog (§8.2)
`transaction.ingested`, `match.found`, `match.auto_confirmed`, `break.created`, `break.assigned`, `break.sla_warning`, `break.sla_breached`, `break.resolved`, `case.approval_requested`, `rule.activated`. Additionally add `match.reconciliation_variance` — not explicitly listed in the doc's catalog (which is marked "etc.", i.e. non-exhaustive) but a direct, necessary consequence of task 19's `RECONCILIATION_VARIANCE` break type; document this addition inline in the code as an intentional catalog extension.

### Data model
```go
type WebhookSubscription struct {
    ID         uuid.UUID
    TenantID   uuid.UUID
    URL        string
    SecretRef  string   // Vault path reference, never inline (16.3 secrets convention)
    EventTypes []string // subset of the catalog above
    FilterExpr *string  // e.g. "amount_at_risk > 50000"
    Status     string   // ACTIVE | PAUSED | DISABLED
    CreatedAt, UpdatedAt time.Time
}

type WebhookDelivery struct {
    ID                   uuid.UUID
    SubscriptionID       uuid.UUID
    EventID              string // outbox event id
    EventType            string
    Payload              []byte
    AttemptCount         int
    Status               string // PENDING | DELIVERED | FAILED | DEAD_LETTERED
    LastAttemptAt        *time.Time
    NextAttemptAt        *time.Time
    ResponseStatus       *int
    ResponseBodySnippet  *string // truncated (e.g. first 2KB) — never store unbounded response bodies
}
```

### Signing
`X-Jengine-Signature: t=<unix_ts>,v1=<hex(hmac_sha256(secret, ts + "." + body))>` (Stripe-style). Including the timestamp in the signed content and requiring the receiver to check clock-skew tolerance prevents replay of a captured valid request. Never sign without the timestamp component.

### Dispatcher flow
`cmd/webhook-dispatcher` consumes `webhook.outbox`. Per event: resolve `ACTIVE` subscriptions for the tenant matching `event_type` and (if present) `FilterExpr`, fan out one `WebhookDelivery` row + one async HTTP POST per matching subscription — never block the consumer loop on a slow tenant endpoint; use a bounded worker pool per delivery attempt.

Retry: exponential backoff with jitter (e.g. 1m, 5m, 30m, 2h, 12h; configurable max attempts, default 8) before marking `DEAD_LETTERED`. Reuse River (already a stack dependency for batch job scheduling per `04-matching-engine.md` §5.2) for retry scheduling rather than hand-rolling a poller — it's already in the dependency graph and this is exactly its use case.

### DLQ + redrive
`DEAD_LETTERED` deliveries are listable via `WebhookService.ListDeliveries` filtered by status; `WebhookService.RedriveDelivery(delivery_id)` resets `attempt_count` and requeues immediately. This RPC is the backend contract the Tenant Admin frontend screen (a separate frontend task) calls — building that UI is out of scope here, but the RPC must exist and be fully functional standalone (verifiable via `grpcurl`/`buf curl` without any UI).

### SSE/WS gateway (ownership assignment — resolves a cross-team gap)
`plans/docs/14-dashboard-frontend.md` §14.1 calls for "a thin WS/SSE gateway" bridging `case.events`/`matching.results` Kafka topics to per-tenant browser sessions, consumed by `plans/task/frontend/10-realtime-sse-integration.md`. No other core task owns this, and it belongs here rather than as a new task: this task already stands up an event-catalog-aware consumer of the same topic family plus an HTTP-facing dispatch surface, so the SSE gateway is a second, small consumer sharing that infrastructure, not a new subsystem.

Build it as an additional entrypoint inside `cmd/webhook-dispatcher` (or a sibling `cmd/realtime-gateway` if keeping HTTP long-lived connections out of the same process as HTTP-outbound retry logic is preferred — pick one and document the choice; either is consistent with the design, this task's author should not leave it unbuilt):
- `GET /v1/tenants/{tenant_id}/events/stream` — an SSE endpoint, one open connection per authenticated session, filtered to `case.events.<shard>`/`matching.results.<shard>` for that tenant (partition/consumer-group scoped to the tenant the same way the dispatcher already resolves subscriptions).
- Auth: validate the same session/bearer mechanism task 15's Connect-RPC auth uses; since browser `EventSource` cannot set an `Authorization` header, support a short-lived, stream-scoped token issued via a small `POST /v1/tenants/{tenant_id}/events/stream-token` RPC (bearer-token-authenticated, normal header) that mints a token valid only for opening the SSE connection, passed as a query parameter — do not reuse a long-lived API token as a URL query parameter.
- Payload shape: reuse the same event-catalog types this task already defines (`break.created`, `match.found`, etc.) — the SSE gateway and the webhook dispatcher are two delivery mechanisms for one shared event model, not two event models.
- No HMAC signing, no retry/backoff, no DLQ for this path — an SSE client that misses an event due to a dropped connection re-syncs via the frontend's REST-based fallback poll (`plans/task/frontend/10`), it does not need guaranteed delivery semantics the way outbound webhooks do.

## Non-Goals / Guardrails
- Do not build the Tenant Admin frontend screen — only the backend RPCs it will call.
- Do not modify task 15's existing `AccountService`/`TransactionService`/`MatchRuleService`/`MatchReviewService`/`BreakService` proto definitions or handlers — `WebhookService` is added independently, see the note above.
- Do not build or modify the SSE/WS real-time gateway that pushes `case.events`/`matching.results` to the frontend (see the new subsection below) as if it were the same thing as webhook delivery — they share the underlying event catalog but are two different consumers of it (one pushes to tenant-configured external URLs, the other pushes to the tenant's own logged-in browser session).
- Do not build the GraphQL reporting gateway (§8.4) — that's V2, see task 26.
- Do not build inbound webhook receiving — that is task 18's webhook-receiver connector, the opposite direction; do not merge or conflate the two "webhook" concepts anywhere in naming or code.
- Do not implement the events this task delivers (`break.created`, `match.auto_confirmed`, etc.) — those are emitted as outbox rows by tasks 19/20; this task only consumes and delivers them.

## Definition of Done
- Unit tests: HMAC sign/verify round-trip, retry-backoff schedule calculation, filter-expression matching against sample event payloads.
- Integration test (`testcontainers-go`: Postgres + Redpanda) simulating an outbox event → dispatcher → mock HTTP receiver, covering both a successful-delivery path and a forced-failure path reaching `DEAD_LETTERED` after max attempts, followed by a successful redrive.
- `buf breaking` passes in CI against the new `webhook.proto`.
- Manual verification: register a subscription pointing at a local mock receiver, trigger a cataloged event, observe signed delivery and a correctly-computed signature on the receiver side.
- SSE gateway: manual verification that opening `/v1/tenants/{id}/events/stream` with a minted stream token receives a `case.events`/`matching.results` event within a reasonable delay after it's produced, and that an unauthenticated/expired-token request is rejected.

## Common Pitfalls
- Storing the webhook secret inline on the subscription row instead of a Vault path reference.
- Signing without a timestamp component, leaving deliveries replayable.
- Making the outbound HTTP call synchronously inside the outbox consumer loop, letting one slow/dead tenant endpoint stall delivery for every other tenant.
- Confusing this task's outbound dispatcher with task 18's inbound webhook-receiver connector — they are unrelated except for sharing a word.
- Blasting every event to every subscription instead of honoring `event_types`/`filter_expr` matching.
- Modifying or "cleaning up" task 15's existing service definitions while in here adding `WebhookService` — stay scoped to the new file.
- Conflating the SSE/WS gateway (below) with webhook delivery — one pushes to external tenant URLs with HMAC signing and retry/DLQ semantics, the other pushes to an authenticated browser session with no signing/retry semantics; don't merge their code paths just because both read the same Kafka topics.
