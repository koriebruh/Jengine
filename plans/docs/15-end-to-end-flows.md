> Part of the Jengine Reconciliation Engine design doc set. Index: [README.md](README.md) · Prev: [14-dashboard-frontend.md](14-dashboard-frontend.md)

# 15 — End-to-End Flows & Glossary

Every other doc in this set describes one component in isolation (the matching engine, the case workflow, the ingestion connectors...). This doc exists so an implementer can follow **one transaction, one rule change, one new tenant, and one failure** through the *entire* system, module by module — the thing that's easy to lose when the design is split across 14 files.

## 15.0 Glossary

| Term | Meaning |
|---|---|
| **Tenant** | One customer institution (a bank/fintech). Everything is scoped to a tenant. |
| **Account** | One bank account / GL ledger book / payment-gateway account being reconciled. |
| **Statement** | One batch of transactions received from one source at one point in time (e.g. one day's MT940 file). |
| **Transaction** | The atomic matchable unit — one line item, from either a batch statement or a streaming event. |
| **MatchRule** | Tenant-configured, versioned config defining how two sets of transactions get compared and scored. |
| **Blocking key** | The field(s) used to bucket transactions into small candidate groups *before* expensive scoring — avoids comparing every transaction to every other transaction. |
| **Scoring** | Weighted composite similarity calculation over a candidate pair/group, producing a confidence score 0–1. |
| **MatchResult / MatchGroup** | The recorded outcome of a rule matching N transactions together (1:1, 1:N, N:1, N:N). |
| **Break / Case** | A transaction (or group) that couldn't be auto-matched or suggested-matched — becomes a human workflow item. |
| **Root cause** | The tagged category explaining *why* a break happened (timing difference, data entry error, etc.). |
| **Maker-checker** | Two-person rule: the person who proposes an action (maker) cannot also approve it (checker). |
| **SLA** | Time budget a break/case must be resolved within, tenant-configured, business-hour-aware. |
| **Temporal workflow** | The durable, replayable process instance backing a Break/Case's lifecycle and any approval gate. |
| **CDC** | Change Data Capture — Debezium streaming Postgres row changes into Kafka for downstream consumers (ClickHouse, audit archive, webhooks). |
| **WORM** | Write-Once-Read-Many — S3 Object Lock compliance mode used for the immutable audit archive. |
| **Idempotency key** | A deterministic hash identifying a record/request so retries/redeliveries never create duplicates. |
| **Isolation tier** | Which multi-tenancy model a tenant uses: Standard (shared+RLS), Isolated Schema, or Dedicated. |

---

## 15.1 Flow: Batch File Ingestion → Match → Case Resolution

The core, most common flow. Referenced docs: [02](02-data-ingestion.md), [03](03-canonical-data-model.md), [04](04-matching-engine.md), [05](05-case-management.md), [06](06-streaming-architecture.md), [14](14-dashboard-frontend.md).

1. Bank drops an MT940 file to the tenant's SFTP path. The SFTP poller connector (`internal/ingestion/connector`, MT940 variant) picks it up on its polling schedule.
2. Connector computes a file checksum, checks it against existing `Statement.checksum` for this account — a duplicate is quarantined or treated as a correction per tenant policy ([02](02-data-ingestion.md) §3.4).
3. MT940 parser (`internal/ingestion/parsers/mt940`) parses the raw file. A `Statement` row is created (`status=RECEIVED`), `raw_file_ref` points at the object-storage copy of the original file.
4. Each parsed line goes through the tenant's field-mapping DSL ([02](02-data-ingestion.md) §3.2) to produce normalized fields.
5. Schema + business validation runs ([02](02-data-ingestion.md) §3.3). Failing lines go to the quarantine queue (visible in the Connector/Ingestion Monitor UI screen, [14](14-dashboard-frontend.md) §14.2). Passing lines continue.
6. An idempotency key is computed per line; Redis bloom filter + `ingestion_dedup` table upsert guards against duplicate processing ([02](02-data-ingestion.md) §3.4).
7. A canonical `Transaction` row is inserted (`status=UNMATCHED`, `source_mode=BATCH`, `statement_id` set). FX normalization applied if the transaction currency differs from the account's base currency ([03](03-canonical-data-model.md) §4.2).
8. A `TransactionEvent` is emitted onto `ingestion.raw.<tenant_shard>` via the transactional outbox pattern ([06](06-streaming-architecture.md) §7.1/§7.3), tagged `source_mode=batch`, carrying `batch_id`.
9. Once all lines are processed, `Statement.status` moves `RECEIVED → PARSED → VALIDATED`.
10. The Batch Matching Engine is triggered (either on a schedule, or on a "statement fully validated" event) and picks up the affected `(tenant_id, account_pair, value_date_bucket)` partitions ([04](04-matching-engine.md) §5.2).
11. Per partition, an in-memory blocking-key index is built from unmatched transactions on both sides, evaluated against active `MatchRule`s in `priority` order.
12. Each candidate gets a confidence score ([04](04-matching-engine.md) §5.1/§5.4):
    - `score >= auto_match` → a `MatchResult(status=AUTO_MATCHED)` is created with its `MatchResultLine`s; both sides' `Transaction.status → MATCHED`.
    - `suggest <= score < auto_match` → `MatchResult(status=SUGGESTED)`, surfaced in the Match Review Queue UI ([14](14-dashboard-frontend.md) screen 4) for a one-click analyst decision.
    - No candidate clears `suggest` for any rule in the chain → the transaction remains `UNMATCHED` after all rules run.
13. Transactions still `UNMATCHED` after the full rule chain become `Break` rows (`status=OPEN`). **Note:** a `SUGGESTED` match is not yet a break — it only becomes one if an analyst explicitly rejects it in the Match Review Queue.
14. `Break` creation starts a **Temporal workflow** ([05](05-case-management.md) §6.1). The workflow's first Activity is Auto-Assignment ([05](05-case-management.md) §6.2), which sets `assigned_to` from the tenant's routing config and computes `sla_due_at` from the tenant's SLA policy.
15. A `break.created` webhook fires ([07](07-api-extensibility.md) §8.2); the Case Queue UI updates live via its SSE channel ([14](14-dashboard-frontend.md) §14.1).
16. An analyst opens the Case Detail screen, reviews linked transactions, optionally runs a manual "wide search" for a low-score candidate, adds comments, tags a root cause, and resolves — or writes off, which routes through the maker-checker `ApprovalWorkflow` child workflow ([05](05-case-management.md) §6.4).
17. On resolution: `Break.status = RESOLVED`, a `CaseAuditEvent` and a global hash-chained `AuditEvent` are both written ([09](09-security-compliance.md) §10.1), and `break.resolved` fires as a webhook.
18. Independently and continuously: Debezium CDC streams every row change above into ClickHouse ([08](08-storage-architecture.md) §9.2/§9.3), feeding the Reconciliation Overview Dashboard ([14](14-dashboard-frontend.md) screen 1) with near-real-time match-rate/break-aging/SLA metrics.

## 15.2 Flow: Streaming Ingestion + Hybrid Reconciliation

Referenced docs: [02](02-data-ingestion.md) §3.5, [04](04-matching-engine.md) §5.2, [06](06-streaming-architecture.md) §7.5.

1. A payment gateway pushes an HMAC-signed settlement webhook to the tenant's webhook-receiver connector.
2. The event runs through the same normalize/validate/idempotency pipeline as batch (steps 4–7 above), except `source_mode=STREAM` and `statement_id` is null.
3. The event lands on `normalized.transactions.<shard>` (partitioned by `tenant_id`+`account_id`).
4. A Streaming Match Consumer — **the same `internal/matching/core` library used by batch**, run from a different entrypoint — looks up the Redis rolling-window candidate pool for the counterpart account and scores against rules whose `execution.mode` includes `streaming`.
5. A match found this way is provisional: `MatchResult(status=AUTO_MATCHED (streaming))`, visible instantly on live dashboard tiles.
6. When the authoritative end-of-day statement later arrives and the batch pass (§15.1 steps 1–12) runs over the same account-pair/date: concordant results get promoted to `AUTO_MATCHED (confirmed)` (final); discordant results (streaming missed a late counterpart, or the fuller batch view found a better pairing) become a `RECONCILIATION_VARIANCE` case — a lightweight review, not a full re-investigation, because the system shows exactly what changed between the streaming and batch outcome.

## 15.3 Flow: Rule Authoring & Activation (Maker-Checker)

Referenced docs: [04](04-matching-engine.md) §5.1/§5.4, [05](05-case-management.md) §6.4, [14](14-dashboard-frontend.md) screen 5.

1. A Recon Manager builds a rule visually in the Rule Builder UI (scope, blocking keys, scoring weights, thresholds); the UI compiles this to the DSL.
2. Before saving as active, the manager runs the **backtesting sandbox**: replays a chosen historical date range read-only against the proposed rule, showing projected auto-match rate, false-positive risk, and break-volume delta — no real `MatchResult` rows are written during this step.
3. The manager submits the rule: `MatchRule(status=DRAFT, created_by=manager)`.
4. A different user with the Approver role reviews and approves: `MatchRule(status=ACTIVE, approved_by=approver, effective_from=now)`. This is the same maker-checker primitive used for break write-offs — reused, not reimplemented.
5. `rule.activated` fires as a webhook. Subsequent batch/streaming matching runs pick up the new rule (rule configs are cached with a short TTL, invalidated on activation).
6. The previous rule version is archived (`status=ARCHIVED`), never deleted — full version history stays queryable for audit.

## 15.4 Flow: Tenant Onboarding

Referenced docs: [01](01-multi-tenancy.md), [02](02-data-ingestion.md) §3.1, [10](10-observability-reliability.md), [14](14-dashboard-frontend.md) screen 7.

1. Platform ops (or self-serve signup) creates a `Tenant` row in the Tenant Registry DB, choosing isolation tier (Standard/Isolated/Dedicated) and region ([01](01-multi-tenancy.md) §2.1, [09](09-security-compliance.md) §10.4).
2. The Tenant Router config is populated (shard/cluster assignment); KMS DEK/KEK provisioned for the tenant ([01](01-multi-tenancy.md) §2.3).
3. Via the Tenant Admin UI, the tenant admin creates initial `Account`s (bank + GL), configures the first `Connector` (credentials stored via Vault-path reference, never inline), sets the field-mapping spec, business/holiday calendar, and SLA policy tiers.
4. The tenant authors (or imports a starter template for) its first `MatchRule`s — goes through the Flow 15.3 approval gate.
5. Webhook subscriptions and RBAC role assignments are configured.
6. The tenant's first statement/connector run exercises Flow 15.1 end-to-end for the first time.

## 15.5 Flow: Failure Handling & Redrive

Referenced docs: [06](06-streaming-architecture.md) §7.1, [10](10-observability-reliability.md) §11.3.

1. Any pipeline stage (connector parse, mapping transform, matching write) that hits an unrecoverable error for a given record publishes that record + full error context to the stage's `dlq.<stage>` topic. **Processing continues for every other record** — one bad record never halts the pipeline.
2. Ops/analyst inspects the DLQ Browser (part of the Connector/Ingestion Monitor UI), sees the payload and error, fixes the root cause (e.g. a bad mapping rule), and manually redrives the record back through the pipeline entrypoint.
3. If the failure was systemic (e.g. a bad rule version mis-scored a whole day), the fix is a **replay**: because every write is idempotency-key-guarded, the affected date range's `normalized.transactions` topic can be safely reprocessed end-to-end with corrected code/rules — no duplicate side effects, no manual cleanup required first.

---
Next: [16-development-workflow.md](16-development-workflow.md)
