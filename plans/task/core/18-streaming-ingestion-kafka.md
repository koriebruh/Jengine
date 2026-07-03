# Task 18: Streaming Ingestion (Kafka/Redpanda)

## Goal
Stand up the real streaming backbone of Jengine: the Redpanda topic layout, the Protobuf event schema managed through Buf with schema-registry enforcement, the transactional-outbox mechanism that reliably moves state changes onto Kafka without dual-write bugs, and the two connectors MVP explicitly deferred — the **webhook-receiver** (inbound, HMAC-verified pushes from payment gateways) and **Kafka-topic-ingestion** (a tenant's own Kafka cluster as a source). This task is the foundation every other V1 task in this phase builds on: streaming matching (19), the outbound webhook dispatcher (21), and the ClickHouse CDC pipeline (22) all depend on the topic layout and outbox pattern established here.

## Prerequisites
- Core task 02 (local dev infra — Redpanda already runs as the local message bus substitute; this task is what actually puts topics/schemas/CDC behind it).
- Core task 03 (schema/migrations — needed for the `outbox_event` table).
- Core task 06 (`SourceConnector` interface + registry).
- Core task 09 (idempotency/validation pipeline — new connectors flow through it unchanged).

## Scope / Deliverables
- `proto/jengine/v1/events.proto` — `TransactionEvent` and any other event messages needed for the topics below; `buf.yaml` / `buf.gen.yaml` if not already present from earlier tooling setup.
- `deploy/redpanda/topics.yaml` (or an `rpk`-driven Make target) declaring every topic in the table below with partition count and retention.
- `deploy/docker-compose.dev.yml` — extend with a Kafka Connect + Debezium container (Redpanda itself already exists from task 02) plus a Debezium **outbox event router** connector config for the outbox pattern.
- `migrations/00xx_outbox_event.sql` — the `outbox_event` table (see below).
- `internal/platform/outbox/` — Go helper for writing outbox rows transactionally alongside a domain-state change (`outbox.Insert(ctx, tx, event)` taking an already-open `*sql.Tx`/pgx transaction).
- `internal/ingestion/connector/webhookreceiver/` — inbound webhook-receiver connector.
- `internal/ingestion/connector/kafkasource/` — Kafka-topic-ingestion connector.
- Wiring both new connectors into the connector registry from task 06.

## Design Reference
- `plans/docs/06-streaming-architecture.md` §7.1 (topic table), §7.2 (Protobuf schema + Buf registry), §7.3 (outbox pattern, idempotent consumers).
- `plans/docs/02-data-ingestion.md` §3.1 (connector interface, the two connectors named here), §3.4 (idempotency — reused unchanged).
- `plans/docs/11-scalability-roadmap.md` §12.1 (over-provision partition counts early — 50–100 partitions per shard topic even at low volume).
- Do not re-read the full rationale here beyond what's needed to implement; open the referenced sections for "why."

## Implementation Notes

### Topics (§7.1)
| Topic | Partition key | Retention |
|---|---|---|
| `ingestion.raw.<shard>` | `tenant_id` | 7 days |
| `normalized.transactions.<shard>` | `(tenant_id, account_id)` | 30 days |
| `matching.results.<shard>` | `(tenant_id, account_id)` | 90 days |
| `case.events.<shard>` | `tenant_id` | 1yr hot |
| `audit.events` | `tenant_id` | compacted, indefinite |
| `webhook.outbox` | `tenant_id` | 7 days |
| `dlq.<stage>` | mirrors source | 90 days |

Create every topic with 50–100 partitions per shard from day one — repartitioning existing data later is expensive, adding partition count is not (§12.1). Tenant-shard-scoped for Standard/Isolated tiers; fully dedicated topic names for Dedicated tier tenants (the Dedicated-tier provisioning logic itself is built in task 24 — here, just make the topic-naming scheme dedicated-tier-aware, e.g. `normalized.transactions.<shard_or_tenant_id>`).

### Event schema
`TransactionEvent` exactly as specified in §7.2 (tenant_id, transaction_id, account_id, value_date, `Money` amount with units/nanos/currency_code, external_ref, counterparty_ref, source_mode enum BATCH|STREAM, idempotency_key, raw_payload as `google.protobuf.Struct`). Register with the Confluent-compatible schema registry Redpanda exposes; `buf breaking` must run in CI against the last-published schema (this wires into task 17's CI pipeline — add the stage if not already present).

### Outbox pattern
```sql
CREATE TABLE outbox_event (
  id           BIGSERIAL PRIMARY KEY,
  tenant_id    UUID NOT NULL,
  aggregate_type TEXT NOT NULL,   -- 'transaction' | 'match_result' | 'break' | 'webhook' | ...
  aggregate_id UUID NOT NULL,
  event_type   TEXT NOT NULL,
  topic        TEXT NOT NULL,
  payload      BYTEA NOT NULL,    -- serialized protobuf message
  created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);
```
Any code path that changes domain state and needs to emit an event inserts into `outbox_event` in the **same DB transaction** as the state change — never a separate produce-to-Kafka call after commit (that's the dual-write bug this pattern exists to avoid). Use the Debezium **outbox event router** single-message-transform (SMT) reading this table's CDC stream and routing rows to the topic named in the `topic` column, keyed by `aggregate_id`. This is the first place Debezium/Kafka Connect appears in the stack — task 22 later reuses the same Kafka Connect cluster and adds table-level CDC connectors for ClickHouse sync; don't stand up a second Kafka Connect cluster in task 22, extend this one.

### Webhook-receiver connector (inbound)
This is the **inbound** direction only — a payment gateway (Stripe/Adyen-style) pushes signed settlement events to Jengine. Do not confuse with task 21's webhook system, which is **outbound** notifications Jengine sends to tenants' systems; they share nothing but the word "webhook."

```go
type WebhookConnectorConfig struct {
    TenantID        string
    HMACSecretRef   string // Vault path, never inline (16.3 secrets convention)
    SignatureHeader string
    SignatureScheme string // "stripe" | "adyen" | "generic-hmac-sha256"
}

type WebhookReceiverConnector struct { /* implements SourceConnector from task 06 */ }

func (w *WebhookReceiverConnector) Fetch(ctx context.Context, cfg ConnectorConfig) (<-chan RawRecord, error)
func (w *WebhookReceiverConnector) Validate(cfg ConnectorConfig) error
func (w *WebhookReceiverConnector) SupportsStreaming() bool { return true }
func (w *WebhookReceiverConnector) Checkpoint() (Cursor, error)

// mounted at /v1/webhooks/ingest/{tenant_id}/{connector_id}
func (w *WebhookReceiverConnector) ServeHTTP(rw http.ResponseWriter, r *http.Request)
```
Edge cases:
- Verify HMAC signature **before** touching the body for anything else (parse, log, store).
- Respond within the sending gateway's timeout budget (aim <5s): verify + enqueue to an internal channel/queue, return 200, process asynchronously. A slow synchronous pipeline behind the HTTP handler will cause the gateway to retry-storm you.
- Dedup by provider delivery-ID header (if present) combined with body hash — feeds the same `ingestion_dedup` mechanism from task 09, don't build a second dedup path.
- Treat the payload as untrusted input like any other connector: it still goes through field-mapping (task 08) and validation (task 09) — this connector's job is only transport + auth, not parsing.

### Kafka-topic-ingestion connector
```go
type KafkaConnectorConfig struct {
    TenantID          string
    BootstrapServers  []string // the tenant's own external Kafka cluster
    Topic             string
    ConsumerGroup     string
    AuthMode          string // SASL_SSL | mTLS
    SchemaFormat      string // "json" | "avro" | "protobuf" — the TENANT'S schema, not Jengine's internal one
}

type KafkaSourceConnector struct { /* implements SourceConnector */ }
```
Use franz-go (already the intended Kafka client per stack choice) to consume the tenant's topic. Important: a tenant's own Kafka messages are **not** assumed to be Jengine's `TransactionEvent` protobuf — they go through the same tenant-configured field-mapping DSL (task 08) as any other source format. Checkpoint via committed consumer-group offsets, exposed through the `Checkpoint()`/`Cursor` mechanism from task 06's interface so redrive/replay tooling works uniformly across connector types.

## Non-Goals / Guardrails
- No ClickHouse or CDC-to-analytics work here — that is task 22, even though it reuses this task's Kafka Connect cluster.
- No outbound webhook dispatch, retry/backoff, or DLQ-redrive UI backend — that is task 21.
- Do not build BAI2, ISO20022, or REST/API-pull connectors here — this task's connector scope is strictly the two named above.
- Do not implement KEDA autoscaling of any consumer here — that belongs to task 19 (streaming matching worker), which is the first thing actually consuming `normalized.transactions.<shard>` at meaningful scale.
- Do not touch the case/workflow system.

## Definition of Done
- Unit tests: HMAC verification (valid, invalid, replayed/stale timestamp), outbox row → topic routing given a fixture `outbox_event` row, Kafka-source connector offset commit/idempotency.
- Integration tests (`testcontainers-go`: Redpanda + Kafka Connect/Debezium + Postgres): a state change writes an `outbox_event` row and the corresponding message is observed on the target topic within a bounded time; a webhook POST with a valid signature results in a canonical `Transaction` row via the existing normalize/validate/dedupe pipeline; an invalid signature is rejected with no side effects.
- `buf breaking` passes in CI against the new `events.proto`.
- Manual verification: `make dev-up` brings up Redpanda + Kafka Connect + the outbox connector; a manually inserted `outbox_event` row appears on its topic via `rpk topic consume`.
- Completion is the test suite passing, not a checklist. Any exploratory QA issues go into a single root-level `QA_REPORT.md` (create if absent), open items only, deleted when fixed.

## Common Pitfalls
- Writing to Postgres and producing to Kafka as two separate operations instead of using the outbox table — reintroduces the exact dual-write bug the design forbids.
- Under-provisioning partitions "since volume is low right now" — contradicts the explicit over-provisioning guidance in §12.1 and is expensive to fix later.
- Sending JSON instead of the registered Protobuf schema on any of the internal topics.
- Blocking the webhook HTTP handler on synchronous downstream processing, causing the sending gateway to retry-storm on timeout.
- Building a second Kafka Connect/Debezium deployment in a later task instead of extending this one.
- Treating the Kafka-topic-ingestion connector's payloads as if they were already Jengine's canonical schema — they must still pass through field mapping like any other source.
