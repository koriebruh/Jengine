> Part of the Jengine Reconciliation Engine design doc set. Index: [README.md](README.md) · Prev: [03-canonical-data-model.md](03-canonical-data-model.md)

# 04 — Matching Engine Design (Core Differentiator)

## 5.1 Rule Engine — Declarative DSL

Rules stored as versioned JSON/YAML (no-code UI rule builder compiles to this DSL; power users/API can author DSL directly):

```yaml
rule:
  name: "Bank vs GL - Standard Match"
  version: 3
  scope:
    source: { account_group: "bank_accounts" }
    target: { account_group: "gl_cash_accounts" }
  match_cardinality: MANY_TO_MANY   # ONE_TO_ONE | ONE_TO_MANY | MANY_TO_ONE | MANY_TO_MANY
  keys:                              # composite blocking key for candidate generation
    - field: value_date
      tolerance: { type: date_window, days: 2 }
    - field: base_amount
      tolerance: { type: numeric, absolute: 0.01, percent: 0.0 }
    - field: currency
      tolerance: exact
  scoring:                           # weighted composite confidence score
    - field: reference
      method: jaro_winkler
      weight: 0.4
      min_similarity: 0.75
    - field: counterparty_ref
      method: levenshtein_normalized
      weight: 0.3
    - field: base_amount
      method: numeric_closeness
      weight: 0.2
    - field: value_date
      method: date_proximity
      weight: 0.1
  thresholds:
    auto_match: 0.92        # auto-confirmed, no human review
    suggest: 0.65           # suggested-match queue
  aggregation_rules:        # for many-to-one/many-to-many
    max_group_size: 20
    sum_tolerance: { absolute: 0.01 }
  execution:
    priority: 10            # lower runs first; unmatched remainder falls through
    mode: [batch, streaming]
```

Key points:
- **Blocking keys** (`keys:`) bucket transactions before expensive pairwise comparison (candidate generation, §5.2 below), separate from scoring logic.
- **Scoring**: pluggable similarity functions via Go function registry (`map[string]ScoringFunc`) — new algorithms don't require schema changes.
- **Cardinality**: one-to-one, one-to-many/many-to-one (e.g. one bank txn = sum of many GL postings), many-to-many (batched settlement vs batched GL entries) — hardest piece is the aggregation solver (§5.2).
- Rules versioned + **approval-gated** (maker-checker on rule changes themselves — a bad rule change can silently misreconcile millions).
- **Rule chaining**: multiple rules run in priority order per account-pair; unmatched residue falls to next rule (exact → tolerance → fuzzy), mirroring analyst workflow.

## 5.2 Matching Algorithm for Scale

**Batch**:
1. Partition by `(tenant_id, account_pair, value_date_bucket)` — matches never cross accounts or far-apart dates, so a day's 1M–50M records split into thousands of independent, fully parallel partitions.
2. Go worker pool (bounded goroutines, sized `GOMAXPROCS × factor`, orchestrated via job queue — River or Asynq on Postgres/Redis) pulls partitions; each worker loads only its partition (bounded working set, e.g. max 50k records), builds in-memory inverted index keyed by rule blocking keys — avoids O(N×M): candidate generation is O(N) hash insert + O(candidates per bucket) scoring.
3. Horizontal scale-out: KEDA autoscaling on job-queue depth — 1M→50M/day is adding worker replicas, not redesign.
4. Many-to-many aggregation: bounded subset-sum/knapsack DP over capped `max_group_size` + rounded-amount buckets; explicit cap avoids combinatorial blowup, falls back to "suggest partial groupings for human review" beyond cap.
5. Result write-back: batch upsert (Postgres COPY/multi-row), not row-by-row — needed to sustain throughput (~580 rec/sec sustained avg at 50M/day, much higher intraday peaks).

**Streaming**:
1. Kafka consumer group per tenant-shard on `normalized.transactions.<shard>`, partitioned by `(tenant_id, account_id)` hash for ordering + locality.
2. Rolling window of unmatched transactions per account-pair in Redis (sorted set) or in-process LRU + periodic Redis sync — e.g. last 7 days as candidate pool.
3. Same blocking-key + scoring logic as batch (**shared Go library `internal/matching/core`** used by both batch workers and streaming consumers — match logic never drifts between modes).
4. Sub-second latency achieved because candidate pool per lookup is small (hundreds–low-thousands, bounded window) and index is in-memory — no full-table scan on hot path.
5. Hybrid reconciliation with batch: streaming matches provisional/fast; nightly batch re-validates against full statement (see [06-streaming-architecture.md](06-streaming-architecture.md) §7.5).

## 5.3 Fuzzy Matching Techniques
- String similarity: Jaro-Winkler (short reference/name strings with typos near start) + normalized Levenshtein/Damerau-Levenshtein (transpositions) — native Go (`internal/matching/similarity`), zero network hop.
- Amount tolerance: absolute + percentage bands, asymmetric tolerance support (e.g. GL short by bank-fee amount).
- Date window: configurable ±N business-day windows, calendar-aware (tenant-configurable holiday calendar per currency/market).
- Composite weighted scoring: final confidence = weighted sum of field similarities, normalized [0,1].
- **ML-based scoring (V2 stretch)**: gradient-boosted model (LightGBM via Go inference binding or ONNX sidecar) trained on historical analyst match/reject decisions, feedback loop from case-resolution outcomes. Explicitly V2, not MVP (cold-start problem needs labeled history first).

## 5.4 Confidence Scoring & Thresholds
- Each candidate → `confidence_score ∈ [0,1]`.
- `>= auto_match` (e.g. 0.92): auto-confirmed, `AUTO_MATCHED`, no human touch — where most speed value is realized (ReconArt's rigid/exact-match-biased engine pushes more to manual review).
- `>= suggest, < auto_match` (e.g. 0.65–0.92): analyst "suggested matches" queue, score + field breakdown shown, one-click confirm/reject.
- `< suggest`: not surfaced by default (avoid alert fatigue); available via explicit "wide search" manual lookup.
- Tenant/rule-configurable thresholds, tunable via **backtesting sandbox** (replay historical day against proposed rule/threshold change, show projected auto-match rate + false-positive risk + break volume before activating) — concrete usability differentiator.

## 5.5 Performance Targets

| Target | Mechanism |
|---|---|
| 50M records/day sustained | Partition-parallel batch workers, autoscaled; bulk COPY writes; blocking-key candidate gen avoids O(N×M) |
| Sub-second streaming latency | Bounded rolling-window candidate pool per account, in-memory/Redis, shared scoring library |
| Avoid N×M blowup | Blocking keys + inverted index → O(N × avg_bucket_size); OpenSearch only secondary assist |
| Predictable p99 under load | Bounded partitions, capped aggregation group size, Kafka-lag-based autoscaling (not unbounded queueing) |
| DB indexing | Composite `(tenant_id, account_id, value_date, base_amount)`; partial index on `status='UNMATCHED'`; BRIN on time-series columns |

---
Next: [05-case-management.md](05-case-management.md)
