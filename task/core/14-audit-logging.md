# Task 14: Audit Logging

## Goal
Build `internal/audit`, the system-wide immutable audit trail writer: a hash-chained `AuditEvent` Postgres table with append-only enforcement at the database-role level, plus a periodic verification job that walks the chain and detects tampering. This is the compliance-grade record that is a superset of every domain event in the system (case transitions from task 13, match confirmations, rule activations, tenant admin actions) — `plans/docs/09-security-compliance.md` §10.1 frames this as the record that must survive even a full application-layer compromise: "retroactive modification breaks the chain, detectable via periodic verification job."

## Prerequisites
- Task 03 (database schema and migrations) — `AuditEvent` table per `plans/docs/03-canonical-data-model.md` §4.1.
- Task 04 (tenancy context and routing) — every `AuditEvent` requires a `tenant_id`; this task consumes `TenantContext` to populate it, not re-derive tenancy logic.

## Scope / Deliverables
- `internal/audit/event.go` — `AuditEvent` struct, `Writer` interface, hash computation.
- `internal/audit/writer.go` — the Postgres-backed `Writer` implementation (synchronous insert, zero-loss-tolerance per SLO).
- `internal/audit/verify.go` — chain verification: walks events in order, recomputes and checks each `hash_chain_prev`/hash link, reports the first break found.
- `internal/audit/cmd_verify.go` or a small `cmd/audit-verify/main.go` — a runnable entrypoint (CLI command or scheduled job) invoking `verify.go`, since this needs to run periodically, not just exist as a library function.
- Migration (coordinate with task 03; add here if not already present): `REVOKE UPDATE, DELETE ON audit_events FROM <app_role>` and confirm only `INSERT`/`SELECT` grants exist for the role the application connects as.

## Design Reference
- `plans/docs/09-security-compliance.md` §10.1 — the exact hash-chaining design: append-only at the application layer *enforced by DB role permissions, not convention*; `hash_chain_prev` links to the previous event's `SHA-256(payload + prev_hash)`; every event captures who (actor + auth method), what (entity + before/after diff), when, where (IP/geo), why (request correlation id).
- `plans/docs/03-canonical-data-model.md` §4.1 `AuditEvent` entity — exact field list: `id (ULID, PK — time-sortable)`, `tenant_id`, `actor_id`, `actor_type`, `event_type`, `entity_type`, `entity_id`, `before_state (jsonb)`, `after_state (jsonb)`, `ip_address`, `request_id`, `occurred_at`, `hash_chain_prev`.
- `plans/docs/10-observability-reliability.md` §11.1 SLO table — "Audit log write durability | Zero loss tolerance — synchronous ack before API success" — the writer must not be fire-and-forget; callers (task 13, task 15) must receive an error if the audit write fails, and that error must propagate as a failure of the originating operation, not be swallowed.
- **Scope note on WORM/S3 archival**: `plans/docs/09-security-compliance.md` §10.1 and `plans/docs/08-storage-architecture.md` §9.4 describe CDC streaming the audit log to WORM object storage near-real-time as part of the same design. `plans/docs/11-scalability-roadmap.md` §12.2 groups "audit hash-chaining + WORM archival" together under Phase 1/V1 in its one-line roadmap summary — but this task's brief is explicit that the hash-chained Postgres table and verification job are MVP scope, and only the WORM/S3 archival streaming is deferred to V1. This task follows that explicit instruction: build the hash chain and verification now (it's cheap — a hash column and an app-layer computation, no new infra), defer the CDC-to-WORM streaming pipeline (real infra: Debezium topic, S3 Object Lock bucket, archival consumer) to V1. Flagged here as a place where the phased-roadmap doc's one-line grouping and this task's brief could read as being in tension — resolved in favor of building the cheap, high-value piece (the chain itself) now.

## Implementation Notes

### AuditEvent and hash computation
```go
type AuditEvent struct {
    ID            string // ULID, time-sortable
    TenantID      uuid.UUID
    ActorID       string
    ActorType     string // USER | SYSTEM | API_KEY
    EventType     string // e.g. "break.transitioned", "match.confirmed", "rule.activated"
    EntityType    string // e.g. "Break", "MatchResult", "MatchRule"
    EntityID      string
    BeforeState   json.RawMessage
    AfterState    json.RawMessage
    IPAddress     string
    RequestID     string
    OccurredAt    time.Time
    HashChainPrev string // previous event's Hash
    Hash          string // this event's own hash
}

func ComputeHash(evt AuditEvent, prevHash string) string {
    // canonical, deterministic serialization of the event's payload fields
    // (exclude Hash itself; include HashChainPrev = prevHash) then SHA-256, hex-encoded
}
```
Canonicalization matters: use a deterministic field order/serialization (e.g. a fixed struct-to-JSON encoding with sorted keys, or explicit field concatenation) so the same logical event always hashes identically — an ambiguous serialization (e.g. Go map iteration order) would make verification unreliable.

### Writer
```go
type Writer interface {
    Write(ctx context.Context, evt AuditEvent) error
}
```
`postgresWriter.Write`:
1. Within a transaction (or `SELECT ... FOR UPDATE`-guarded read of the last row), read the current chain tail's `Hash` for this... **decide and document explicitly: is the hash chain per-tenant or global?** The schema (`tenant_id` on every row) and the topic design (`audit.events` topic keyed by `tenant_id`, `plans/docs/06-streaming-architecture.md` §7.1) both suggest per-tenant chains are the more natural and operationally sound choice (a tenant's chain can be verified/exported independently, matches data-residency/export-per-tenant requirements). Implement **per-tenant hash chains** (chain tail tracked per `tenant_id`), not one global chain across all tenants — note this as the resolved design choice since the doc doesn't spell it out explicitly.
2. Compute `Hash = ComputeHash(evt, prevHash)`.
3. Insert the row. This insert must be synchronous and its error returned to the caller — per the zero-loss-tolerance SLO, a failed audit write should fail the originating operation (task 13's `Transition`, task 15's mutating handlers), not be logged-and-ignored.
4. Concurrency: two concurrent writes for the same tenant racing for "current chain tail" is a real risk (task 13 and task 15 both call this). Use `SELECT ... FOR UPDATE` on a small per-tenant "chain tail" tracking row (or `SERIALIZABLE` isolation with retry) to serialize chain-extension per tenant — document the chosen concurrency-control mechanism clearly, since getting this wrong silently corrupts the chain under concurrent load (a race here doesn't crash anything, it just produces a chain that verification will later report as broken, which is worse than an obvious failure).

### Verification job
```go
type VerificationReport struct {
    TenantID      uuid.UUID
    EventsChecked int
    FirstBreakAt  *string // event ID where the chain first fails to verify, nil if clean
}

func VerifyChain(ctx context.Context, store Store, tenantID uuid.UUID) (VerificationReport, error)
```
Walks events for a tenant in `ID` (ULID, time-sortable) order, recomputes `ComputeHash` for each against the previous event's stored `Hash`, compares to the stored `HashChainPrev`/`Hash` of the current row, and reports the first mismatch. Run this as a periodic job (simple cron-style scheduled invocation is fine at MVP — no need for a workflow engine here) across all tenants, and expose it as a manually-invokable command for on-demand compliance checks.

### DB role enforcement
The actual enforcement of append-only is a `GRANT`/`REVOKE` at the Postgres role level, not application code: the role the application's connection pool authenticates as gets `INSERT`, `SELECT` on `audit_events` and explicitly **not** `UPDATE`/`DELETE`. Verify this is true with a test that attempts an `UPDATE`/`DELETE` as the app role and asserts it's rejected by Postgres itself (not just by application logic) — this is the actual guarantee the design relies on ("no UPDATE/DELETE grants for app DB role — Postgres role permission enforcement, not just convention").

## Non-Goals / Guardrails
- No CDC-to-WORM streaming, no S3 Object Lock integration, no Debezium `audit.events` topic wiring — that is V1 (`plans/docs/08-storage-architecture.md` §9.4, `plans/docs/09-security-compliance.md` §10.1's WORM-copy-as-source-of-truth goal is a V1 deliverable, not built here). The Postgres table is the sole source of truth at MVP.
- No OPA/ABAC policy enforcement on who can read/query the audit log (task 23, V1) — this task only guarantees write-append-only-ness and read access via whatever the API layer (task 15) exposes at a coarse RBAC level, if at all.
- No quarterly access-review report generation, no SOC2 Type II audit tooling (§10.2) — out of scope for this task entirely; not currently assigned a task number in 10-17.
- No field-level tokenization/redaction for GDPR erasure tension (`plans/docs/08-storage-architecture.md` §9.4) — that's a later refinement once archival (V1) exists; nothing to redact into yet at MVP since there's no archival copy.
- Do not build this as a fire-and-forget async writer (e.g. writing to a channel and returning immediately) — violates the zero-loss-tolerance synchronous-ack SLO.

## Definition of Done
- Unit tests for `ComputeHash` determinism (same logical event always produces the same hash; a single-byte change in any field changes the hash).
- Unit test proving chain verification detects a tampered row (directly mutate a row in a test DB bypassing the app role's restrictions — e.g. via a superuser test connection — and confirm `VerifyChain` reports the break at the correct event).
- Integration test (testcontainers-go, real Postgres with the actual role grants applied): confirm the app-role connection cannot `UPDATE`/`DELETE` `audit_events` (expect a Postgres permission-denied error, not an application-level check).
- Concurrency test: fire concurrent `Write` calls for the same tenant and confirm the resulting chain is still valid (no lost updates, no two events claiming the same `hash_chain_prev`).
- `go test -race ./internal/audit/...` passes.
- Manual verification: run the verification job against the local dev stack's seeded data (task 02) and confirm a clean report; manually tamper a row via a superuser connection and confirm the next run detects it.
- Completion is tests passing; exploratory QA issues go in root-level `QA_REPORT.md`, open items only, deleted when fixed.

## Common Pitfalls
- Computing the hash chain globally across all tenants instead of per-tenant — makes per-tenant export/verification (a real compliance requirement) awkward and creates unnecessary cross-tenant write contention on the chain tail. Chain per `tenant_id`.
- Relying only on application-level checks ("the code never calls UPDATE on this table") to claim append-only-ness — the design explicitly requires DB-role-level enforcement; a test that only checks application behavior without checking actual Postgres grants doesn't verify the real guarantee.
- Serializing the hash input non-deterministically (e.g. hashing a Go struct via a JSON encoder without sorted/fixed field order, or including a field like a DB-generated timestamp with sub-millisecond nondeterminism) — makes verification unreliable in subtle, hard-to-debug ways.
- Making the audit write async or best-effort "for performance" — directly violates the zero-loss-tolerance SLO in §11.1; if audit write latency is a real concern, that's a scaling problem to solve later (e.g. batching within the same transaction), not a reason to drop the synchronous guarantee.
- Building the WORM/S3 archival piece anyway "since it's mentioned right next to hash-chaining in the roadmap doc" — re-read the Design Reference note above; this task's explicit brief defers it, don't scope-creep into V1 infra work.
