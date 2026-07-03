# Task 15: REST API Layer (MVP)

## Goal
Build the MVP public API surface: the essential CRUD and action endpoints needed to exercise the full MVP flow end-to-end — accounts, statements (read), transactions (read), match rules (author/activate, minimal), match review actions (confirm/reject suggested matches), and breaks/cases (assign/comment/transition, calling into task 13's `LifecycleService`). Every mutating endpoint supports the `Idempotency-Key` header from day one. This is the layer the MVP frontend (`plans/task/frontend/`) and any early API-integrating design partner will actually call.

## Prerequisites
- Task 03 (database schema and migrations), Task 04 (tenancy context and routing), Task 05 (canonical domain models and repositories) — this layer is a thin handler layer over existing repositories and tenancy context, not a new data layer.
- Task 11 (matching rule DSL) — the rule-authoring endpoint parses/compiles rule YAML/JSON via this package.
- Task 12 (matching batch worker) — surfaces `MatchResult` rows it produces for the match-review endpoints.
- Task 13 (case/break lifecycle MVP) — breaks/cases endpoints are thin wrappers over `cases.LifecycleService`.
- Task 14 (audit logging) — every mutating handler must produce an `AuditEvent` (directly, or transitively via task 13's `LifecycleService`, which already writes it — don't double-write).

## Scope / Deliverables
- `proto/jengine/v1/*.proto` — `AccountService`, `StatementService`, `TransactionService`, `MatchRuleService`, `MatchReviewService`, `CaseService` (or a consolidated `BreakService`) — see Design Reference for why this is proto-first, not hand-rolled REST.
- `internal/apiserver/` (or `cmd/coreapi/api/`) — Connect-RPC service implementations backing the above, one file per service.
- `internal/apiserver/idempotency.go` — the `Idempotency-Key` interceptor/middleware and its backing store.
- `internal/apiserver/auth.go` — MVP auth: JWT/API-key → `TenantContext` resolution (delegates to task 04's tenancy package; this task wires it into the HTTP/Connect layer, doesn't reimplement it).
- Migration: `idempotency_requests` table (tenant_id, idempotency_key, request_hash, response_status, response_body, created_at, unique on (tenant_id, idempotency_key)).
- `buf.gen.yaml` / `buf.yaml` — Buf config for proto lint/breaking-change checks and code generation (Go server stubs; TypeScript client generation is the frontend's concern, not built here).

## Design Reference — and a resolved ambiguity, read this first
`plans/docs/11-scalability-roadmap.md` §12.2 Phase 0 says MVP needs "Basic REST API, no webhooks/GraphQL yet," which could be read as "skip the proto/Connect-RPC design entirely and hand-roll `net/http`." **This task does not read it that way, and here is the reasoning, since it's a judgment call other parallel work should be aware of:**
- `plans/docs/00-overview-and-architecture.md` §1.3 tech-stack table lists the API framework as "gRPC (internal) + Connect-RPC (grpc-web/REST bridge) + thin `net/http`/chi for REST/webhooks" and "Protobuf everywhere (APIs + Kafka events) via Buf schema registry" as **the** contract technology — not qualified as V1-only.
- `plans/docs/13-implementation-notes.md` explicitly lists `proto/jengine/v1/transaction.proto` as one of the very first files to scaffold at Phase 0 kickoff, calling it "the source of truth for both gRPC APIs and Kafka event contracts."
- `plans/docs/16-development-workflow.md` §16.5 CI pipeline stage 3 (`buf breaking`) is listed unconditionally, not gated behind a V1 flag.
- Connect-RPC (bufbuild/connect-go) serves gRPC, gRPC-Web, **and plain-JSON-over-HTTP from one `.proto`-defined implementation** — so building the MVP's "basic REST API" via Connect-RPC *is* building a REST API from a client's point of view (plain JSON over HTTP, no gRPC client required), while avoiding a second hand-rolled REST codepath that would need to be kept in sync with the proto contract later.

**Resolution: build MVP endpoints contract-first via `.proto` + Connect-RPC from day one.** What §12.2 Phase 0 actually defers is *feature scope* — no webhook delivery system (task 21, V1), no GraphQL reporting gateway (V2, `plans/docs/11-scalability-roadmap.md` §12.2 Phase 2) — not the underlying contract technology. This is a deliberate call flagged in this task's final report for reconciliation against other parallel tasks' assumptions (particularly the frontend's API-client-codegen task, `plans/task/frontend/02`, which should generate its TypeScript client from these same `.proto` files rather than hand-typing a REST client).

Further references:
- `plans/docs/07-api-extensibility.md` §8.1 — resource-oriented REST conventions (`/v1/tenants/{id}/accounts`), pagination via `page_token`, `Idempotency-Key` header requirement, versioning via URL path.
- `plans/docs/01-multi-tenancy.md` §2.2 — tenant resolution from JWT claim or API key, threaded via `TenantContext`.
- `plans/docs/14-dashboard-frontend.md` §14.4 — MVP frontend has no rule-builder UI; rules are authored "as raw YAML/JSON via API/CLI at MVP" — this is why a `MatchRuleService` endpoint (even a minimal one) is in this task's scope despite not being explicitly named in the one-line MVP task description; without it there is no way to get a rule from a YAML file into the running system apart from direct DB seeding, which breaks the "exercise the MVP flows" goal of this task.

## Implementation Notes

### Services and endpoints (MVP set)
- `AccountService`: `CreateAccount`, `GetAccount`, `ListAccounts` (per tenant).
- `StatementService`: `GetStatement`, `ListStatements` — read-only; statements are created by ingestion (tasks 06-09), not via this API.
- `TransactionService`: `GetTransaction`, `ListTransactions` (filterable by account, status, date range) — read-only for MVP; no direct transaction mutation via API.
- `MatchRuleService`: `CreateDraftRule` (accepts raw YAML or JSON body, parses/compiles via task 11's `ParseYAML`/`Compile`, persists `MatchRule(status=DRAFT)`), `ActivateRule` (maker-checker-lite: requires `approved_by` != `created_by`, sets `status=ACTIVE`, `effective_from=now`, archives the previous active version), `ListRules`, `GetRule`. This is intentionally minimal — no backtesting sandbox (deferred, see task 11 Non-Goals), no rule-builder-shaped nested editing API (that's what the V1 Rule Builder UI would need, not MVP raw YAML authoring).
- `MatchReviewService`: `ListSuggestedMatches` (filter `MatchResult.status=SUGGESTED`), `ConfirmMatch`, `RejectMatch` (rejecting a suggested match is what turns its transactions into a `Break` — call `cases.LifecycleService`/`BreakSink` the same way task 12's unmatched residue does; do not build a second break-creation path), `BulkConfirmMatches` (required per `plans/docs/14-dashboard-frontend.md` §14.2 screen 4: "bulk-confirm for high-confidence batches" — frontend task 05's bulk-confirm bar depends on this; returns a per-ID result, mirroring task 13's `BulkResult` shape, since a bulk confirm over a mixed selection can partially fail, e.g. a candidate already confirmed/rejected by another analyst concurrently).
- `BreakService` (or `CaseService`): `ListBreaks` (filter by status/assignee/account/priority), `GetBreak`, `AssignBreak`, `AddComment`, `TransitionBreak` (wraps `cases.LifecycleService.Transition`), `RequestApproval`, `DecideApproval`, `TagRootCause`, `BulkAssignBreaks`, `BulkCommentBreaks`, `BulkResolveBreaks` (thin wrappers over task 13's `BulkAssign`/`BulkAddComment`/`BulkTransition` — required per `plans/docs/05-case-management.md` §6.2's explicit "multi-select assign/comment/resolve" requirement; frontend task 03's row-selection bulk toolbar calls these directly, do not omit them as this task's first pass did before a cross-task audit caught the gap).

Every mutating RPC (`Create*`, `Activate*`, `Confirm*`, `Reject*`, `Assign*`, `Transition*`, `AddComment`, `RequestApproval`, `DecideApproval`, `Bulk*`) reads `Idempotency-Key` from the request header (Connect-RPC interceptors have header access) — see idempotency handling below. Every mutating RPC's handler is a thin adapter: validate input, resolve `TenantContext`, delegate to the appropriate repository/service (task 05 repos, task 11's compiler, task 13's `LifecycleService`), and let those layers own the actual audit-event writes (task 14) — handlers should not re-implement audit writing themselves where the called service already does it (task 13's methods already write `AuditEvent`; a handler calling `LifecycleService.Transition` must not also directly call `audit.Writer.Write` for the same logical event). For `Bulk*` RPCs specifically, the response body must surface the per-ID success/failure breakdown (task 13's `BulkResult` shape) rather than collapsing it into a single success/error — the frontend's bulk-action bar needs to show the user exactly which selected rows failed and why, not just "some failed."

### Idempotency-Key handling
```go
type IdempotencyStore interface {
    Get(ctx context.Context, tenantID uuid.UUID, key string) (*StoredResponse, error)
    Save(ctx context.Context, tenantID uuid.UUID, key string, requestHash string, resp StoredResponse) error
}

type StoredResponse struct {
    StatusCode int
    Body       []byte
}
```
Interceptor logic for mutating RPCs:
1. If no `Idempotency-Key` header present, proceed without idempotency guarantees (or require it — decide per-endpoint; requiring it on genuinely destructive actions like `ActivateRule`/`TransitionBreak` is safer, and is consistent with §8.1 calling this "critical to prevent double-submission on client retry" — default to requiring it on all mutating RPCs for MVP simplicity, one rule rather than a per-endpoint policy to remember).
2. Compute `request_hash` (hash of method + tenant + request body).
3. Look up `(tenant_id, idempotency_key)`. If found and `request_hash` matches: return the stored response without re-executing. If found and `request_hash` differs: reject with an error (key reused for a different request — this is a client bug, surface it clearly, don't silently execute either version). If not found: execute the handler, then store the response keyed by `(tenant_id, idempotency_key)`.
4. Storage TTL: keep it simple at MVP (e.g. no automatic expiry, or a generous fixed TTL like 24h cleaned by a periodic job) — don't over-engineer eviction for MVP.

### Auth (MVP scope)
Resolve `tenant_id` from a JWT claim or API key exactly per `plans/docs/01-multi-tenancy.md` §2.2 (delegating to task 04's tenancy package for the actual `tenant_id` → shard/config lookup) and thread it into `context.Context` as `TenantContext` before any handler runs. MVP auth is intentionally coarse: authenticate the caller and resolve tenant + a basic role claim, but do not build OIDC/SAML SSO integration or OPA/ABAC policy evaluation — that's task 23 (V1). A simple RBAC role check (e.g. "must be at least Analyst role to call `AssignBreak`") is reasonable if trivial to add, but is not required for MVP sign-off.

## Non-Goals / Guardrails
- No webhook system (event catalog, delivery, HMAC signing, retry/DLQ/redrive UI) — that is task 21 (V1), per `plans/docs/07-api-extensibility.md` §8.2.
- No GraphQL reporting gateway — that is V2 (`plans/docs/11-scalability-roadmap.md` §12.2 Phase 2), not even V1; do not build it here or defer-stub it.
- No Connector SDK / WASM plugin surface (`plans/docs/07-api-extensibility.md` §8.3) — task 25, V1.
- No OIDC/SAML SSO, no OPA/ABAC policy evaluation, no scoped API-key management UI — task 23, V1. MVP auth is a coarse JWT/API-key → tenant + role resolution only.
- No backtesting-sandbox endpoint for rules (would require re-running historical data read-only — a larger feature not in the 10-17 task range; note as a known gap).
- No frontend TypeScript client generation — that's `plans/task/frontend/02`'s job, consuming the `.proto` files this task produces via `buf generate`.
- Do not hand-roll a second, parallel plain-`net/http` REST implementation alongside the Connect-RPC one "to be safe" — Connect-RPC already serves REST/JSON; a second implementation is duplicated surface that will drift.

## Definition of Done
- Unit tests per service handler (request validation, tenant-context propagation, error mapping).
- Idempotency interceptor unit tests: same key + same body → cached response returned, handler not re-executed (assert via a call-count spy); same key + different body → rejected; missing key on a mutating endpoint → rejected (or documented default behavior, per the decision above).
- Integration tests (testcontainers-go, real Postgres): full request → handler → repository/service → DB round trip for at least one representative endpoint per service (e.g. `CreateDraftRule` → `ActivateRule` → visible via `GetRule`; `ListSuggestedMatches` → `ConfirmMatch` → `Transaction.status` updated; `AssignBreak` → `AddComment` → visible via `GetBreak`).
- `buf lint` and `buf breaking` (against an empty/initial baseline, since this is the first proto commit) pass as part of this task's own verification, anticipating task 17's CI wiring.
- `go test -race ./internal/apiserver/... ./proto/...` passes.
- Manual verification: run `cmd/coreapi` against the local dev stack (task 02), issue plain HTTP/JSON requests (e.g. via `curl` or Postman) to each MVP endpoint, and confirm responses match the proto-defined JSON mapping without needing a gRPC client.
- Integration test for `BulkAssignBreaks`/`BulkCommentBreaks`/`BulkResolveBreaks`/`BulkConfirmMatches`: a mixed-status selection produces a response with correct per-ID success/failure breakdown, and the underlying `LifecycleService`/match-confirmation calls write exactly one audit event per batch op, not one per item.
- Completion is tests passing; exploratory QA issues go in root-level `QA_REPORT.md`, open items only, deleted when fixed.

## Common Pitfalls
- Omitting bulk endpoints (`BulkAssignBreaks`/`BulkCommentBreaks`/`BulkResolveBreaks`/`BulkConfirmMatches`) because the one-line MVP task description doesn't mention them — the design docs (`05-case-management.md` §6.2, `14-dashboard-frontend.md` §14.2 screen 4) require them explicitly and the frontend screens are built assuming they exist; this was an actual gap caught in a cross-task audit of this task set, not a hypothetical risk.
- Collapsing a bulk endpoint's response into a single pass/fail instead of a per-ID breakdown — a bulk action over a heterogeneous selection (mixed current statuses) legitimately partially fails, and the frontend needs to show exactly which rows failed and why.
- Reading "basic REST API, no webhooks/GraphQL yet" as license to skip proto/Buf entirely and hand-roll routes — re-read the Design Reference resolution above; proto-first via Connect-RPC is how this design achieves "basic REST" without a second throwaway implementation.
- Building `MatchRuleService.ActivateRule` without enforcing `approved_by != created_by` — this is the MVP-lite form of maker-checker described in `plans/docs/04-matching-engine.md` §5.1 ("a bad rule change can silently misreconcile millions") and skipping it defeats the point even at MVP scale.
- Implementing `RejectMatch` as a separate, bespoke break-creation code path instead of routing through the same `BreakSink`/`LifecycleService.OpenBreak` call task 12 uses for unmatched residue — creates two divergent ways breaks come into existence.
- Making `Idempotency-Key` an optional nicety implemented as an afterthought on one or two endpoints — the brief and §8.1 both call out that retrofitting this later is painful; wire the interceptor generically across all mutating RPCs from the start.
- Handlers directly calling `audit.Writer.Write` for events that the underlying service (task 13's `LifecycleService`) already writes — produces duplicate `AuditEvent` rows for the same logical action, corrupting the audit trail's meaning (each event should represent one real occurrence).
- Building GraphQL "since it's related to reporting and seemed easy to add" — it's explicitly V2, two phases away from this task; do not build it.
