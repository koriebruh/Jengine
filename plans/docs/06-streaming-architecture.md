> Part of the Jengine Reconciliation Engine design doc set. Index: [README.md](README.md) · Prev: [05-case-management.md](05-case-management.md)

# 06 — Real-Time/Streaming Architecture

## 7.1 Kafka Topic Design

| Topic | Partition key | Purpose | Retention |
|---|---|---|---|
| `ingestion.raw.<shard>` | tenant_id | Raw ingested records pre-normalization | 7 days |
| `normalized.transactions.<shard>` | (tenant_id, account_id) | Canonical events, streaming matcher input | 30 days |
| `matching.results.<shard>` | (tenant_id, account_id) | Match/suggestion outcomes | 90 days |
| `case.events.<shard>` | tenant_id | Case state transitions | 1yr hot, archived after |
| `audit.events` | tenant_id | CDC outbox for audit log, fans to WORM + ClickHouse | Compacted + tiered indefinitely |
| `webhook.outbox` | tenant_id | Transactional outbox for webhook delivery | 7 days |
| `dlq.*` | mirrors source | Failed records after retries, manually reprocessable | 90 days |

Tenant-shard-scoped topics for Standard/Isolated tiers; fully dedicated topics for Dedicated tier.

## 7.2 Event Schema (Protobuf via Buf)
```protobuf
message TransactionEvent {
  string tenant_id = 1;
  string transaction_id = 2;
  string account_id = 3;
  google.protobuf.Timestamp value_date = 4;
  Money amount = 5;          // { units, nanos, currency_code } — avoids float precision issues
  string external_ref = 6;
  string counterparty_ref = 7;
  SourceMode source_mode = 8; // BATCH | STREAM
  string idempotency_key = 9;
  google.protobuf.Struct raw_payload = 10;
}
```
Schema Registry (Confluent-compatible, works with Redpanda) enforces backward compatibility; `buf breaking` in CI rejects incompatible changes before merge.

## 7.3 Exactly-Once / Effectively-Once
Pragmatic model: **at-least-once delivery + idempotent consumers**, not true distributed exactly-once.
- Idempotent producers (`enable.idempotence=true`).
- Consumers upsert via `idempotency_key` unique constraint — replays absorbed.
- Multi-step consumer logic uses **transactional outbox pattern**: state change + "next event to emit" in same Postgres transaction, Debezium outbox connector publishes reliably — avoids dual-write inconsistency without Kafka transactions end-to-end.

## 7.4 Backpressure
- Consumer-group lag drives KEDA autoscaling of streaming-matcher workers.
- If lag exceeds tenant SLA-risk threshold: prioritize high-priority accounts (configurable tiers), lower-priority traffic buffers in topic retention (no data loss, delayed matching, alert fired).
- Redis rolling-window state has bounded memory (TTL/LRU eviction); any missed-window matches caught by nightly batch pass (§7.5).

## 7.5 Hybrid Batch/Streaming Reconciliation Model
Key architectural answer for coexistence (gap in ReconArt's batch-only model):
1. Streaming matches are **provisional** (`AUTO_MATCHED (streaming)`) — near-real-time visibility (e.g. "98% of today's settlements already matched, live").
2. When authoritative batch statement arrives, full batch pass runs over complete dataset using same core matching library (see [04-matching-engine.md](04-matching-engine.md)).
3. **Reconciliation-of-reconciliations**: batch results diffed against streaming results. Concordant → promoted to `AUTO_MATCHED (confirmed)` (final). Discordant (streaming missed/mismatched due to late counterpart or narrower window) → flagged `RECONCILIATION_VARIANCE` for lightweight review (system shows exactly what changed).
4. Gives tenants both real-time operational visibility and end-of-day authoritative correctness — marketed as "continuous reconciliation."

---
Next: [07-api-extensibility.md](07-api-extensibility.md)
