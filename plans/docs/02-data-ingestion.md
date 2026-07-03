> Part of the Jengine Reconciliation Engine design doc set. Index: [README.md](README.md) · Prev: [01-multi-tenancy.md](01-multi-tenancy.md)

# 02 — Data Ingestion Layer

## 3.1 Connector Architecture

Plugin-based framework, common Go interface:
```go
type SourceConnector interface {
    Fetch(ctx context.Context, cfg ConnectorConfig) (<-chan RawRecord, error)
    Validate(cfg ConnectorConfig) error
    SupportsStreaming() bool
    Checkpoint() (Cursor, error)
}
```

Built-in connectors at launch: SFTP/FTP poller (batch, incl. inline PGP decrypt), CSV/Excel/fixed-width upload (streaming parser, no full-file memory load), SWIFT MT940/MT942/MT101/MT103 (batch, per-bank dialect mapping table), BAI2 (batch), ISO 20022 camt.053/054 (batch/streaming, XML), REST/API pull (configurable auth/pagination/JSONPath mapping), Kafka topic ingestion (streaming, tenant can point own producers directly), webhook receiver (streaming, HMAC-verified inbound HTTPS, e.g. Stripe/Adyen settlement pushes), JDBC/DB pull (batch, watermark cursor or full Debezium CDC if tenant grants replication access).

**Third-party extensibility**: connectors distributed as **WASM (TinyGo/wazero)** rather than native `.so` plugins — sandboxes untrusted third-party code (critical for multi-tenant financial ingestion), avoids Go plugin ABI fragility across versions. Connector SDK (see [07-api-extensibility.md](07-api-extensibility.md) §8.3) exposes stable WASM host-function interface for I/O, secrets, record emission.

## 3.2 Schema Mapping / Normalization
Pipeline: `Raw Fetch → Format Parse → Field Mapping (tenant-configured) → Normalization → Validation → Dedup/Idempotency → Canonicalization → Persist + Emit Event`.

Field mapping = tenant-configurable versioned JSON/YAML spec with transform functions (date parse, currency normalize, sign-flip, trim/case), expressed as small mapping DSL — lets non-technical ops onboard new source formats without code. Example:

```yaml
source_format: MT940
mappings:
  - target: transaction.amount
    source: field_61.amount
    transform: [parse_decimal, apply_sign_from(field_61.debit_credit_mark)]
  - target: transaction.currency
    source: field_61.currency
    transform: [uppercase, iso4217_validate]
  - target: transaction.value_date
    source: field_61.value_date
    transform: [parse_date("YYMMDD")]
  - target: transaction.reference
    source: field_86.narrative
    transform: [extract_regex("REF:(\\S+)")]
```

## 3.3 Data Validation
- Schema validation (required fields, types/formats). Business validation (configurable per tenant, e.g. amount sign rules, account allowlist).
- Failures land in **quarantine queue** with raw payload + reason, surfaced for manual remediation — never silently drop financial data.

## 3.4 Idempotency & Dedup
- Idempotency key = `hash(tenant_id, source_connector_id, source_natural_key_or_record_hash, ingestion_batch_id)`.
- Redis bloom filter (fast negative check) + durable `ingestion_dedup` Postgres table (unique constraint, upsert) for authoritative dedup.
- File re-upload detection via file-hash + filename + date heuristics; configurable per tenant whether re-send is rejected or treated as correction.
- Model: at-least-once delivery + idempotent writes (not distributed exactly-once) — simpler, sufficient given upsert guards.

## 3.5 Batch + Streaming Convergence
Both paths emit same canonical event onto per-tenant Kafka topic (`ingestion.raw.<tenant_shard>`), tagged `source_mode=batch|streaming`. Downstream normalization/matching consumers are mode-agnostic; Matching Engine orchestration layer tracks whether a tenant/account-pair's "batch window" is open (see [06-streaming-architecture.md](06-streaming-architecture.md) §7.5). One codepath serves both worlds — deliberately hard for ReconArt's batch-only architecture to retrofit.

---
Next: [03-canonical-data-model.md](03-canonical-data-model.md)
