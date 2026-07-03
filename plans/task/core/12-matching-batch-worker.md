# Task 12: Matching Batch Worker

## Goal
Build `cmd/matching-batch`, the deployable binary that actually runs matching over ingested transactions at scale. It partitions work, pulls partitions off a job queue, loads each partition's transactions, invokes `internal/matching/core.Match` (task 10) with rules compiled by `internal/matching/rules` (task 11), and writes results back — including turning unmatched residue into `Break` rows via the `core.BreakSink` interface (implemented by task 13). This is the MVP realization of `plans/docs/04-matching-engine.md` §5.2's "Batch" algorithm: no KEDA/Kubernetes autoscaling yet, but correctly partitioned and horizontally runnable (multiple worker processes can safely run against the same job queue today, which is what makes adding KEDA later an infra change, not a redesign).

## Prerequisites
- Task 05 (canonical domain models and repositories) — loads `Transaction` rows, writes `MatchResult`/`MatchResultLine` rows.
- Task 09 (idempotency and validation) — this worker only operates on transactions that have already passed ingestion validation and are `status=UNMATCHED`.
- Task 10 (matching engine core library) — the `Match` function and `BreakSink` interface this worker calls.
- Task 11 (matching rule DSL) — compiled `CompiledRule`s and the scoring registry this worker loads and passes to `Match`.
- Task 13 (case/break lifecycle MVP) provides the concrete `BreakSink` implementation this worker's `main.go` wires in — task 12 and task 13 can be built in either order relative to each other as long as the `BreakSink` interface (defined in task 10) is respected, but the worker cannot be fully wired/run end-to-end until task 13's implementation exists.

## Scope / Deliverables
- `cmd/matching-batch/main.go` — binary entrypoint: config load, dependency wiring (repositories, scoring registry, job queue client, `BreakSink` implementation from task 13), worker pool startup.
- `internal/matching/batch/partition.go` — partition key computation and partition enumeration (`(tenant_id, account_pair, value_date_bucket)`).
- `internal/matching/batch/worker.go` — the bounded worker pool: claims partitions from the job queue, loads bounded working sets, calls `core.Match`, dispatches results.
- `internal/matching/batch/writeback.go` — batch upsert of `MatchResult`/`MatchResultLine` rows and `Transaction.status` updates via Postgres `COPY`/multi-row upsert (not row-by-row).
- `internal/matching/batch/jobs.go` — job queue producer/consumer wiring (enqueue a partition job on trigger, consume and process).
- Migration (if not already covered by task 03/09): a job-queue-backing table if using River (River manages its own schema via its migration helper — invoke that, don't hand-roll the table).

## Design Reference
- `plans/docs/04-matching-engine.md` §5.2 "Batch" — the five numbered steps this task implements almost directly: partition by `(tenant_id, account_pair, value_date_bucket)`, bounded worker pool sized `GOMAXPROCS × factor`, bounded per-partition working set (max 50k records), horizontal scale-out via job-queue depth (MVP: manual/process-count scale-out; KEDA itself is V1 infra, not this task), batch upsert write-back.
- `plans/docs/01-multi-tenancy.md` §2.4 — mentions "batch jobs via KEDA-scaled worker pools consuming tenant-partitioned Kafka topics or job queue (Asynq/River)" — this task builds the job-queue-driven MVP form (no Kafka topic consumption yet; that's task 18/19, V1).
- `plans/docs/15-end-to-end-flows.md` §15.1 steps 9-13 — exactly what triggers a batch run ("statement fully validated" event or schedule) and what happens to each classification outcome (`AUTO_MATCHED`, `SUGGESTED`, residual `UNMATCHED` → `Break`).
- `plans/docs/16-development-workflow.md` §16.1 module-boundary rule — `matching-batch` imports `internal/matching/core` and, only in `main.go`, `internal/cases` to construct the concrete `BreakSink` — it must not import `internal/cases` from `internal/matching/batch/*.go`.

## Implementation Notes

### Job queue choice: River (deliberate call — flag for reconciliation)
The design docs offer "River or Asynq" without picking one (`plans/docs/04-matching-engine.md` §5.2, `plans/docs/01-multi-tenancy.md` §2.4). This task picks **River** (Postgres-native job queue): it lets partition-job enqueue happen in the same Postgres transaction that marks a `Statement` `VALIDATED` (consistent with the transactional-outbox pattern used elsewhere in this design, e.g. CDC outbox in §7.3), and avoids adding a Redis dependency purely for job orchestration when Redis is already reserved elsewhere in this design for caching/rate-limiting/idempotency (`plans/docs/01-multi-tenancy.md` §2.4, `plans/docs/00-overview-and-architecture.md` §1.3). If another parallel task has already made a different, incompatible choice, reconcile before merging — this is called out explicitly in the final report for this reason.

### Partitioning
```go
type PartitionKey struct {
    TenantID        uuid.UUID
    SourceAccountID uuid.UUID
    TargetAccountID uuid.UUID
    ValueDateBucket time.Time // truncated to day (or configurable bucket width)
}
```
`EnumeratePartitions(ctx, since time.Time) ([]PartitionKey, error)` queries for distinct `(tenant_id, account pairs implied by active rules' scope, value_date)` combinations with `status='UNMATCHED'` transactions newer than the last successful run watermark. Account pairing comes from each `CompiledRule`'s `scope.source`/`scope.target` account groups (task 11) — a partition exists per rule-relevant account pair, not a blind cross-product of all accounts.

### Worker pool
- Bounded goroutine pool sized `runtime.GOMAXPROCS(0) * factor` (factor configurable, default small e.g. 2-4).
- Each worker claims one partition job at a time from River, loads only that partition's transactions (bounded working set — enforce a max record count per partition load, e.g. 50k, matching §5.2; if a partition exceeds this, split it further by a sub-bucket, e.g. narrower date range, rather than loading unbounded rows into memory).
- Load both sides of the partition (source account's unmatched transactions, target account's unmatched transactions) plus the active `CompiledRule`s in priority order for that account pair (loaded via task 11's compile path from stored `MatchRule.rule_spec`, cached with short TTL to avoid recompiling every partition).
- Call `core.Match(ctx, source, target, rules, registry)`.

### Write-back
- `AUTO_MATCHED` and `SUGGESTED` candidates from `MatchOutcome`: batch-insert `MatchResult` + `MatchResultLine` rows via `COPY` or multi-row `INSERT ... VALUES (...), (...)` — never row-by-row inserts in the hot path, per §5.2 point 5.
- Update `Transaction.status` for all involved transaction IDs in a single batched `UPDATE ... WHERE id = ANY($1)` per outcome type, not per-row.
- `MatchOutcome.Unmatched` residue: after all rules for the partition have run, transactions still unmatched call `BreakSink.OpenBreak` (one call per logical break — group residue by whatever the tenant's break-grouping policy is; MVP default: one break per unmatched transaction, not a bulk merge, to keep task 13 simple — note bulk/grouped break creation as a possible refinement, not required for MVP).
- All of the above for one partition should happen in a single Postgres transaction where practical (result writes + status updates), so a partial-partition-failure doesn't leave transactions half-updated; `BreakSink.OpenBreak` calls (task 13's responsibility) can be outside that transaction since case creation is its own bounded operation — document this consistency boundary clearly in code comments since it's a common source of subtle bugs.

### Trigger
MVP trigger: either (a) a scheduled tick (e.g. every N minutes, configurable) that calls `EnumeratePartitions` and enqueues jobs for any with new unmatched transactions, or (b) enqueued directly when a `Statement` transitions to `VALIDATED` (task 09's ingestion pipeline enqueues a River job in the same transaction as the status update — this is the transactional-outbox-consistent option and is the better default; implement this, with the scheduled tick as a safety-net catch-all for streaming-sourced or otherwise statement-less transactions).

## Non-Goals / Guardrails
- No KEDA/Kubernetes autoscaling, no consumer-group-lag-based scaling — this task's job is correct partitioning + horizontal-runnability (running N processes of this binary against the same job queue must be safe), not autoscaling infrastructure (that's V1 deploy/infra work, not numbered 10-17).
- No Kafka/Redpanda consumption — this worker reads from Postgres (the job queue and the transaction table), not from `normalized.transactions.<shard>`. That's `cmd/matching-stream`, task 19 (V1).
- No direct import of `internal/cases` outside `main.go` — see module-boundary rule above.
- No many-to-many aggregation, no ML scoring — inherited from tasks 10/11's scope.
- No hybrid batch/streaming reconciliation-of-reconciliations logic (`plans/docs/06-streaming-architecture.md` §7.5) — there is no streaming path yet at MVP for this worker to reconcile against.
- Do not build a bespoke job queue from scratch — use River (or reconcile to Asynq if that's the convention established elsewhere) rather than hand-rolling polling/locking logic.

## Definition of Done
- Unit tests for `EnumeratePartitions` (partition boundaries, bucket-width edge cases) and `writeback.go` (batch upsert correctness against a real Postgres via testcontainers, not mocked).
- Integration test (testcontainers-go per `plans/docs/16-development-workflow.md` §16.4): seed a Postgres with a small tenant, two accounts, a handful of transactions and one active rule; run the worker end-to-end; assert expected `MatchResult` rows, `Transaction.status` updates, and `Break` rows (via a test double or the real task-13 `BreakSink` implementation) are correct.
- Load a synthetic partition of a few thousand records and confirm processing completes without loading unbounded memory (sanity-check the 50k-record working-set cap is actually enforced, not just documented).
- `go test -race ./internal/matching/batch/... ./cmd/matching-batch/...` passes.
- Manual verification: run `cmd/matching-batch` against the local dev stack (task 02) with seeded fixture data (task 02's `make seed`) and confirm a full batch pass produces the expected match/break outcomes end-to-end.
- Completion is tests passing; exploratory QA issues go in root-level `QA_REPORT.md`, open items only, deleted when fixed.

## Common Pitfalls
- Loading an entire tenant's unmatched transactions into memory "to keep the code simple" instead of respecting per-partition bounded working sets — defeats the scalability design point of this whole module and will not surface as a bug until production volume.
- Row-by-row `INSERT`/`UPDATE` in the write-back path — works fine in a test with 10 rows, falls over at the 580 rec/sec sustained (much higher intraday peak) target from §5.5.
- Reaching into `internal/cases` internals directly from `internal/matching/batch` because it's "just one function call" — this is exactly the shortcut the module-boundary rule in `plans/docs/16-development-workflow.md` §16.1 exists to prevent; use the `BreakSink` interface and wire the concrete type only in `main.go`.
- Treating a `SUGGESTED` match's constituent transactions as unmatched residue and opening a `Break` for them — they are not residue; only genuinely `Unmatched` (never cleared `suggest` for any rule) becomes a `Break`, per `plans/docs/15-end-to-end-flows.md` §15.1 step 13.
- Silently picking a different job queue (or hand-rolled polling) than what task 13/other parallel tasks assume, causing integration friction — the River choice here is a deliberate but reconcilable call; check for conflicts before treating it as final.
