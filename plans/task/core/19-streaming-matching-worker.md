# Task 19: Streaming Matching Worker

## Goal
Build `cmd/matching-stream`: the streaming consumer that matches transactions in near-real-time as they arrive, using the exact same scoring/blocking logic as the batch engine (`internal/matching/core`, built in core task 10) against a bounded Redis rolling-window candidate pool. This delivers Jengine's real-time-recon differentiator over ReconArt's batch-only model — but the harder and more important half of this task is the **hybrid batch/streaming reconciliation model**: streaming matches are provisional, and the nightly batch pass is the authoritative source of truth that reconciles against them. Get the hybrid model wrong and the platform either loses the "continuous reconciliation" story or silently produces false-confidence matches. Treat this as core, not a nice-to-have.

## Prerequisites
- Core task 18 (Kafka topics, event schema, outbox — `normalized.transactions.<shard>` must exist and be populated).
- Core task 10 (`internal/matching/core` — the shared blocking/scoring library; this task must not reimplement or fork it).
- Core task 12 (batch matching worker — this task's reconciliation logic runs after a batch pass completes).
- Core task 13 (case lifecycle — `RECONCILIATION_VARIANCE` discordance produces a Break/Case).

## Scope / Deliverables
- `cmd/matching-stream/main.go` — the streaming consumer entrypoint.
- `internal/matching/stream/` — consumer loop, candidate-pool client, priority/backpressure handling.
- `internal/matching/stream/pool.go` — `CandidatePool` interface + Redis-backed implementation.
- `internal/matching/reconcile/` — the batch-vs-streaming reconciliation job.
- `migrations/00xx_match_result_streaming_status.sql` — additive migration adding `AUTO_MATCHED_STREAMING` and `AUTO_MATCHED_CONFIRMED` to `MatchResult.status`, and `RECONCILIATION_VARIANCE` to `Break.break_type` (expand-only; never drop/rename existing enum values).
- `deploy/helm/matching-stream/` — KEDA `ScaledObject` manifest scaling on Kafka consumer-group lag.

## Design Reference
- `plans/docs/04-matching-engine.md` §5.2 (streaming algorithm: consumer group per tenant-shard, rolling window, shared `internal/matching/core`), §5.4 (confidence thresholds — unchanged, reused as-is).
- `plans/docs/06-streaming-architecture.md` §7.4 (backpressure/KEDA), §7.5 (the hybrid model — read this in full before writing the reconciliation job).
- `plans/docs/15-end-to-end-flows.md` §15.2 (concrete walkthrough of this exact flow, useful as an acceptance-test script).
- `plans/docs/03-canonical-data-model.md` (`MatchResult`, `Break` fields).

## Implementation Notes

### Candidate pool
```go
type CandidatePool interface {
    Add(ctx context.Context, tenantID, accountID string, rec *matchingcore.CandidateRecord) error
    Query(ctx context.Context, tenantID, accountID string, key matchingcore.BlockingKey) ([]*matchingcore.CandidateRecord, error)
    Remove(ctx context.Context, tenantID, accountID, txnID string) error
}
```
Redis sorted-set implementation: key = `cand:{tenant_id}:{account_pair_hash}`, score = `value_date` epoch, member = serialized candidate reference. Window is bounded (e.g. last 7 days, tenant-configurable) via TTL/trim-on-write, not unbounded growth — an evicted-before-matched candidate is **expected and fine**, it gets caught by the nightly batch pass (§7.5 point 3); do not add special-case "extend the window" logic to avoid this, it's an intentional part of the hybrid design.

### Consumer
- One consumer group per tenant-shard on `normalized.transactions.<shard>`, already partitioned by `(tenant_id, account_id)` from task 18.
- Reuse `internal/matching/core`'s blocking + scoring engine unchanged — construct it with the Redis-backed `CandidatePool` as its candidate source instead of the batch worker's in-memory partition index. If the batch engine's public API doesn't already support pluggable candidate sources, that is itself a signal the task-10 abstraction needs a small interface extension here — do not fork the scoring logic to work around it.
- Only rules with `execution.mode` including `streaming` (per the DSL from task 11) run in this path.
- A match clearing `auto_match` threshold here is written as `MatchResult(status=AUTO_MATCHED_STREAMING)` — **provisional**, never treated as final. This status must never be shown to a tenant as a closed/confirmed match in any API response without a "provisional, pending batch confirmation" qualifier.
- Concurrency: serialize processing per `(tenant_id, account_id)` pair (e.g. hash to N keyed worker goroutines) to avoid races on the same Redis pool key from two goroutines simultaneously — do not rely on a single global mutex, and do not process the same account-pair's events out of order across goroutines.

### Backpressure / KEDA (§7.4)
- Export consumer-group lag as a Prometheus gauge; KEDA `ScaledObject` scales `matching-stream` replicas on that lag metric.
- If lag exceeds a tenant's SLA-risk threshold, prioritize high-priority accounts/tenants (tenant-configurable tiers) — implement via separate consumer deployments per priority tier, or via pausing low-priority partition fetches (franz-go supports pausing fetch on specific partitions) when lag crosses the threshold. Do not implement unbounded in-memory queueing as a backpressure strategy — the design explicitly prefers "buffer in topic retention, alert, catch up later" over unbounded consumer-side queues.

### Hybrid reconciliation (§7.5) — the critical part
```go
type ReconciliationJob struct {
    TenantID        string
    AccountPairID   string
    ValueDateBucket time.Time
}

func (r *Reconciler) ReconcileBatchAgainstStream(ctx context.Context, job ReconciliationJob) error
```
Triggered after the batch pass (task 12) completes for a given `(tenant_id, account_pair, value_date_bucket)` partition — wire this as a hook/event the batch worker fires on partition completion, not a rewrite of the batch worker itself.

Steps:
1. For every batch-produced `MatchResult` in the partition, look up whether a `MatchResult(status=AUTO_MATCHED_STREAMING)` already exists covering an overlapping transaction set (join through `MatchResultLine`).
2. **Concordant** (same transaction grouping, same-or-better confidence): update status to `AUTO_MATCHED_CONFIRMED` (final). Write an `outbox_event` for `match.auto_confirmed` (task 18's outbox mechanism — task 21 consumes it for the actual webhook send).
3. **Discordant** (streaming matched a different/no counterpart than batch found, or batch found a grouping streaming missed due to a late counterpart or narrower window): create a `Break(break_type=RECONCILIATION_VARIANCE, ...)`. This is deliberately a **lightweight review case**, not a full re-investigation — the system must show exactly what changed between the streaming and batch outcome (store both result snapshots, e.g. `Break.metadata` or a dedicated diff record, so the Case Detail UI — a later frontend task — can render the delta).
4. Transactions that were never matched streaming-side simply flow through the normal batch path (§15.1) — no special handling needed, this is not a discordance, it's the expected common case for anything outside the streaming window.

## Non-Goals / Guardrails
- Do not reimplement or duplicate blocking/scoring logic — any divergence between batch and streaming scoring is treated as a correctness bug in this design, not an acceptable tradeoff.
- Do not treat `AUTO_MATCHED_STREAMING` as a terminal/final state anywhere in the codebase.
- Do not build the batch matching worker itself (task 12 already exists) — only the post-batch reconciliation hook.
- Do not implement ML-based scoring (explicitly V2, see task 26).
- Do not install or configure a KEDA/Kubernetes cluster from scratch — assume KEDA is available platform infra; this task only ships the `ScaledObject` manifest and the lag metric it reads.
- Do not build the actual webhook delivery mechanism for `match.auto_confirmed` — only emit the outbox event; task 21 owns delivery.

## Definition of Done
- Golden-dataset tests (extending `internal/matching/core/testdata/`) covering concordant and multiple discordant scenarios (late counterpart, narrower streaming window, conflicting grouping), asserting correct status transitions and `Break` creation.
- Integration tests (`testcontainers-go`: Redpanda + Redis + Postgres) simulating a streaming match followed by a batch pass over the same partition, asserting the reconciliation outcome end-to-end.
- Unit tests for candidate-pool TTL/eviction behavior.
- Property-based test confirming the candidate pool's memory stays bounded under sustained load (LRU/TTL eviction actually fires, doesn't just theoretically exist).
- Manual verification: publish a synthetic streaming event, observe a provisional `AUTO_MATCHED_STREAMING` result, then run a batch pass over the same window and observe promotion to `AUTO_MATCHED_CONFIRMED`.

## Common Pitfalls
- Building a second, subtly-different scoring implementation for the streaming path "because it needs to be faster" — the design explicitly calls out this drift risk; speed comes from the bounded candidate pool, not a different algorithm.
- Treating `AUTO_MATCHED_STREAMING` as final in any dashboard, API response, or webhook payload — breaks the entire "continuous reconciliation" story.
- Letting the Redis rolling window grow unbounded "to avoid missing late matches" — missed streaming-side matches are supposed to be caught by batch reconciliation, not prevented by an ever-growing window.
- Processing two events for the same account-pair concurrently without serialization, causing a race on the shared candidate pool.
- Forgetting `RECONCILIATION_VARIANCE` needs to be an additive enum migration, not a repurposing of an existing `break_type` value.
- Making the reconciliation job re-run the full matching engine instead of doing a targeted diff/join against existing results.
