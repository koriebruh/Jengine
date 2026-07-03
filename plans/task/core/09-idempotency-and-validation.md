# Task 09: Idempotency and Validation

## Goal
Implement the pipeline's Validation stage (schema + business rules) and Dedup/Idempotency stage (task 06's pipeline stages 5-6), completing the ingestion pipeline so it can be trusted with production financial data: nothing invalid or duplicate is silently persisted, and any pipeline stage is safely re-runnable without creating duplicate side effects. This closes out the MVP ingestion layer (tasks 06-09) — after this task, ingested data is validated, deduplicated, and ready for the matching engine (task 10+) to consume with confidence in data integrity.

## Prerequisites
Task 06 (pipeline framework — the `Stage` interface and `QuarantineSink` this task implements against). Task 03 (database schema — the `ingestion_dedup` table this task's dedup stage writes to already exists from task 03's migration). Task 08 (field mapping/normalization — validation runs against normalized fields, so this task's validators consume task 08's `NormalizedFields` output shape).

## Scope / Deliverables
- `internal/ingestion/validation/schema.go` — schema validation (required fields, types/formats).
- `internal/ingestion/validation/business.go` — tenant-configurable business validation rules (amount sign rules, account allowlist, etc.).
- `internal/ingestion/validation/stage.go` — wires both into task 06's `Stage` interface for the Validation pipeline stage.
- `internal/ingestion/dedup/idempotency.go` — idempotency key computation.
- `internal/ingestion/dedup/bloomfilter.go` — Redis bloom filter fast-negative-check layer.
- `internal/ingestion/dedup/dedup.go` — the authoritative `ingestion_dedup` Postgres-table-backed dedup stage, implementing task 06's `Stage` interface for the Dedup pipeline stage.
- Fixture-driven tests for both stages, including a re-upload/replay scenario proving idempotent re-processing.

## Design Reference
- plans/docs/02-data-ingestion.md §3.3 (validation: schema validation for required fields/types/formats; business validation configurable per tenant, e.g. amount sign rules, account allowlist; failures land in quarantine queue with raw payload + reason, never silently dropped) and §3.4 (idempotency & dedup: idempotency key = `hash(tenant_id, source_connector_id, source_natural_key_or_record_hash, ingestion_batch_id)`; Redis bloom filter fast negative check + durable `ingestion_dedup` Postgres table with unique constraint/upsert for authoritative dedup; file re-upload detection via file-hash+filename+date heuristics, configurable reject-vs-correction per tenant; model is at-least-once delivery + idempotent writes, NOT distributed exactly-once).
- plans/docs/10-observability-reliability.md §11.3 (idempotent replay/reprocessing: idempotency keys + sufficient Kafka retention mean any pipeline stage is safely re-runnable — this task's idempotency-key design is what makes that property true, even though full replay tooling/DLQ redrive UX is a later/V1 concern).
- plans/docs/15-end-to-end-flows.md §15.1 steps 5-6 (concrete walk-through: failing lines go to quarantine, visible in the Connector/Ingestion Monitor UI — frontend task, out of scope here, just needs the queryable sink from task 06; idempotency key computed per line, Redis bloom filter + `ingestion_dedup` table upsert guards against duplicate processing) and §15.5 (failure handling: one bad record never halts the pipeline; systemic failures are fixed via replay, safe because every write is idempotency-key-guarded).
- Task 03 (03-database-schema-and-migrations.md) — the `ingestion_dedup` table's exact column shape (`id, tenant_id, idempotency_key, source_connector_id, ingestion_batch_id, created_at, UNIQUE (tenant_id, idempotency_key)`) this task's dedup stage writes/queries against.

## Implementation Notes
- **Idempotency key computation** (`idempotency.go`):
  ```go
  func ComputeIdempotencyKey(tenantID uuid.UUID, connectorID uuid.UUID, naturalKeyOrRecordHash string, batchID uuid.UUID) string {
      h := sha256.New()
      h.Write([]byte(tenantID.String()))
      h.Write([]byte{0})
      h.Write([]byte(connectorID.String()))
      h.Write([]byte{0})
      h.Write([]byte(naturalKeyOrRecordHash))
      h.Write([]byte{0})
      h.Write([]byte(batchID.String()))
      return hex.EncodeToString(h.Sum(nil))
  }
  ```
  Use `{0}` (null byte) separators between components to avoid ambiguous concatenation collisions (e.g. `"ab"+"c"` vs `"a"+"bc"`). `naturalKeyOrRecordHash`: prefer a genuine source natural key when the format provides one (e.g. MT940's own transaction reference field if present and reliably unique); fall back to a hash of the full normalized record's stable fields (excluding volatile fields like `received_at`) when no natural key exists — implement a deterministic `RecordHash(fields NormalizedFields) string` helper (sort map keys before hashing if fields are held as a map, to keep the hash stable across runs) for this fallback path. Document which source formats use which path (MT940: prefer field_61's own reference if the parser/mapping surfaces one; else fallback).
  This computed key is what gets written into `Transaction.ingestion_idempotency_key` (task 05's schema) and into `ingestion_dedup.idempotency_key`.
- **Redis bloom filter** (`bloomfilter.go`): use Redis's `BF.ADD`/`BF.EXISTS` (RedisBloom module) if available in the local Redis image, OR implement a simple bloom filter in Go backed by a Redis bitset (`SETBIT`/`GETBIT` + multiple hash functions) if RedisBloom isn't part of task 02's plain `redis:7` image — check task 02's compose file choice first; if it's plain Redis (no RedisBloom module), implement the Go-side bitset bloom filter rather than silently requiring a Redis module task 02 never provisioned. This is a concrete decision point: default to the portable Go-side bitset implementation unless task 02's Redis image is confirmed to include RedisBloom, since introducing an unplanned Redis module dependency this late in the sequence is worse than a self-contained bloom filter.
  ```go
  type BloomFilter interface {
      MayExist(ctx context.Context, tenantID uuid.UUID, key string) (bool, error) // false = definitely not seen; true = maybe seen (must confirm against ingestion_dedup)
      Add(ctx context.Context, tenantID uuid.UUID, key string) error
  }
  ```
  Bloom filter is a fast-path negative check only — a `false` result skips the Postgres round-trip entirely (record is definitely new); a `true` result always falls through to the authoritative Postgres check (bloom filters have false positives, never false negatives, by construction — implement/size it accordingly, e.g. target <1% false-positive rate at expected per-tenant daily volume).
- **Authoritative dedup** (`dedup.go`): implements task 06's `Stage` interface for the Dedup pipeline stage:
  ```go
  func (d *DedupStage) Process(ctx context.Context, rec *pipeline.PipelineRecord) (pipeline.StageResult, error) {
      key := ComputeIdempotencyKey(...)
      maybeExists, _ := d.bloom.MayExist(ctx, tenantID, key)
      if maybeExists {
          exists, err := d.repo.ExistsByIdempotencyKey(ctx, tenantID, key) // task 05's TransactionRepository method
          if exists { return pipeline.StageDrop, nil } // logged drop, not silent — see below
      }
      // Insert into ingestion_dedup via upsert (ON CONFLICT (tenant_id, idempotency_key) DO NOTHING),
      // checking rows-affected to detect a race (two concurrent workers processing the "same" record).
      inserted, err := d.dedupRepo.TryInsert(ctx, tenantID, key, connectorID, batchID)
      if !inserted { return pipeline.StageDrop, nil }
      d.bloom.Add(ctx, tenantID, key)
      rec.IdempotencyKey = key
      return pipeline.StageContinue, nil
  }
  ```
  The `ingestion_dedup` unique-constraint upsert (`ON CONFLICT (tenant_id, idempotency_key) DO NOTHING`, checking affected-row-count) is the authoritative guard — the bloom filter is purely a performance optimization to skip the Postgres round-trip for the common case of a genuinely new record; correctness must hold even if the bloom filter were disabled entirely (test this explicitly — see Definition of Done).
  A `StageDrop` result from dedup must be logged (structured log with tenant_id/connector_id/idempotency_key/reason="duplicate") — per task 06's `StageDrop` semantics ("deliberate skip... must be logged, never silent") — this is the concrete case that documented requirement exists for.
  File re-upload detection (file-hash + filename + date heuristics) is layered above the per-line idempotency-key dedup: it operates at the `Statement`/file level (already partly implemented in task 07's connector-level checksum check) — this task's `dedup.go` should expose a `CheckFileReupload(ctx, tenantID, accountID, checksum, filename string) (ReuploadPolicy, error)` helper that task 07's connectors call, returning `Reject` or `TreatAsCorrection` per tenant config (`tenant_settings` from task 03/04), so the policy decision logic lives in one place (this task) rather than being duplicated per-connector.
- **Schema validation** (`schema.go`): validates `NormalizedFields` (task 08's output) has all required fields present (non-empty `currency`, `value_date`, non-nil `amount`, valid `account_id` reference), correct types/formats (currency is 3-char ISO 4217 — reuse task 08's `iso4217_validate`, don't reimplement). Returns a list of `ValidationError{Field, Reason}` — any non-empty list → `StageQuarantine` with a reason string joining the errors.
- **Business validation** (`business.go`): tenant-configurable rules loaded from `tenant_settings`/a dedicated `business_validation_rules` config (JSONB spec — reuse the small-DSL philosophy from task 08 rather than inventing a new one; a rule here is simpler, e.g. `{"rule": "amount_sign", "side": "DEBIT", "allow_negative": false}` or `{"rule": "account_allowlist", "allowed_account_ids": [...]}`). Implement at least: amount-sign-consistency check (side vs. sign agreement) and account-allowlist check (transaction's account_id must be an active `Account` for the tenant) as the two concrete rules named in §3.3 — the mechanism should be extensible (a small registry similar to task 08's transform registry) but do not build out a large rule library beyond these two named examples plus whatever's needed for the fixture tests.
- Concurrency: dedup's `TryInsert` must be safe under concurrent workers processing what might be the same record simultaneously (two SFTP poll cycles racing, or two pipeline workers) — rely on the Postgres unique constraint (task 03) as the actual race-safe guard, never on an in-process check-then-insert without the DB constraint backing it. The bloom filter must also be safe for concurrent access (Redis operations are inherently safe; if using the Go-side bitset, ensure `SETBIT`/`GETBIT` calls don't need additional client-side locking since Redis serializes them).

## Non-Goals / Guardrails
- Do not implement true distributed exactly-once delivery (transactional Kafka consumers, two-phase commit, etc.) — §3.4 explicitly states the model is at-least-once delivery + idempotent writes; building exactly-once semantics here would be over-engineering against the stated design.
- Do not implement the DLQ browser UI, manual redrive tooling UI, or the broader replay/reprocessing operational tooling — those are observability/ops features (plans/docs/10-observability-reliability.md §11.3, frontend connector-monitor screen) layered on top of this task's idempotency guarantees, not built by this task. This task only needs to make replay *safe*, not build the replay *tooling*.
- Do not implement a general business-rule DSL/engine beyond the two named rule types (amount-sign, account-allowlist) plus straightforward extensibility — do not gold-plate this into a full rules engine; that class of complexity belongs to the actual Matching Rule DSL (task 11), which is a different, more complex system serving a different purpose.
- Do not add a RedisBloom module dependency without first confirming task 02's compose file actually provisions it — default to the portable Go-side bitset bloom filter implementation unless RedisBloom is confirmed present, per Implementation Notes.
- Do not persist quarantined/dropped records' full raw payload insecurely if they might contain sensitive fields (e.g. card data per plans/docs/09-security-compliance.md §10.2) — while full tokenization is a V1/security-hardening task (23), this task's quarantine writes should still avoid needlessly duplicating obviously-sensitive raw fields beyond what task 06's `QuarantineEntry.RawPayload` already scopes; do not add new sensitive-data-handling logic here, just don't make it worse.

## Definition of Done
- Unit tests: `ComputeIdempotencyKey` is deterministic (same inputs → same key) and sensitive to each input component (changing any one of tenant/connector/natural-key/batch changes the key); `RecordHash` fallback is stable regardless of map key iteration order.
- Unit tests: bloom filter `MayExist`/`Add` round-trip; a false-positive-rate test at a realistic fixture size stays under the target threshold (e.g. seed N known keys, check M unknown keys, assert false-positive rate roughly matches configured target within tolerance).
- Integration test (`testcontainers-go` Postgres + Redis): the SAME record processed twice through the full Dedup stage results in exactly one `Transaction`/`ingestion_dedup` row — run once with the bloom filter artificially disabled/bypassed to prove the Postgres unique-constraint path alone is sufficient for correctness (this is the concrete test proving the bloom filter is a pure optimization, not a correctness dependency).
- Integration test: schema validation rejects a record missing a required field (e.g. no `currency`) and routes it to quarantine with a reason mentioning the missing field; business validation rejects a transaction against a non-allowlisted account and routes it to quarantine with a reason naming the account-allowlist rule.
- Integration test: re-uploading an identical source file (same checksum) through task 07's connector is either rejected or treated as a correction per tenant policy, exercised both ways.
- End-to-end pipeline test (tasks 06-09 together): task 07's MT940 fixture, mapped via task 08, validated and deduped by this task, produces exactly the expected set of `Transaction` rows with correct `ingestion_idempotency_key` values — and re-running the identical fixture through the pipeline a second time produces zero additional `Transaction` rows.
- Manual verification: deliberately feeding a malformed CSV row (missing currency) through `make seed`'s pipeline path results in a queryable quarantine entry, not a crash and not a silently-dropped record.

## Common Pitfalls
- Treating the Redis bloom filter as the authoritative dedup mechanism (skipping the Postgres unique-constraint check when the bloom filter says "not seen") — a false negative is impossible by bloom-filter construction, but a *false positive* on a "maybe seen" result must always fall through to the authoritative check; more importantly, correctness must never depend on the bloom filter being present/correct at all — it's a fast-path optimization only.
- Implementing dedup as an application-level "check then insert" without the Postgres unique constraint actually enforcing atomicity — under concurrent processing this is a classic TOCTOU race that creates duplicate `Transaction` rows exactly in the high-volume scenario this task exists to prevent.
- Building schema/business validation before the field-mapping stage's output is fully normalized (validating raw un-normalized fields instead of task 08's `NormalizedFields`) — validation must run against normalized data per the pipeline order task 06 established (Field Mapping → Normalization → Validation).
- Silently dropping duplicate or invalid records with only a log line instead of a queryable quarantine entry / logged `StageDrop` — plans/docs/02-data-ingestion.md §3.3 is explicit: "never silently drop financial data."
- Conflating file-level re-upload detection (task 07's checksum check) with line-level idempotency dedup (this task) as if they were the same mechanism — they operate at different granularities and both are required; this task should centralize the file-reupload *policy decision* even though task 07 computes the file checksum.
- Over-building the business-validation rule mechanism into something resembling the full Matching Rule DSL (task 11) — keep it deliberately simpler; it validates individual transactions against straightforward per-tenant constraints, it does not score/compare pairs of transactions.
