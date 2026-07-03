# Task 06: Ingestion Connector Framework

## Goal
Build the extensibility contract for data ingestion: the `SourceConnector` Go interface, a connector registry, and the pipeline-stage orchestration (Raw Fetch → Format Parse → Field Mapping → Normalization → Validation → Dedup → Canonicalization → Persist + Emit Event) that every concrete connector (task 07) and normalization component (task 08/09) plugs into. Per plans/docs/13-implementation-notes.md, `internal/ingestion/connector.go` is called out as one of the first files to scaffold — it "defines the extensibility contract from day one," including the third-party WASM connector SDK path that arrives in V1 (task 25), so getting the interface shape stable now avoids a breaking change later.

## Prerequisites
Task 05 (canonical domain models and repositories — the pipeline persists `Statement`/`Transaction` rows via those repositories, and reads/writes `Connector` config via `ConnectorRepository`). Task 04 (tenancy — every connector run is tenant-scoped).

## Scope / Deliverables
- `internal/ingestion/connector/connector.go` — the `SourceConnector` interface, `RawRecord`/`ConnectorConfig`/`Cursor` types.
- `internal/ingestion/connector/registry.go` — connector registry (type name → constructor), registration mechanism.
- `internal/ingestion/pipeline/pipeline.go` — the orchestrator implementing the 8-stage pipeline as a composable sequence of stage interfaces, wired to per-tenant Kafka-shaped emission (local dev: Redpanda from task 02) at the final stage.
- `internal/ingestion/pipeline/stage.go` — the `Stage` interface each pipeline step implements, plus the shared `PipelineRecord` type threaded between stages (carries raw data at stage 1, progressively enriched fields through stage 8).
- `internal/ingestion/quarantine.go` — quarantine queue interface (concrete failing-record persistence; task 09 wires actual validation failures into it, this task defines the sink contract and a Postgres-backed default implementation).
- Unit tests + a fake/no-op connector implementation (`internal/ingestion/connector/testconnector/` or similar) used to exercise the pipeline end-to-end without depending on task 07's real connectors.

## Design Reference
- plans/docs/02-data-ingestion.md §3.1 (the exact `SourceConnector` interface signature to implement verbatim — do not redesign it) and §3.2 (the 8-stage pipeline sequence — implement stages in this exact order, each stage independently testable/composable) and §3.5 (batch+streaming convergence: both paths must emit the same canonical event onto `ingestion.raw.<tenant_shard>`, tagged `source_mode=batch|streaming` — the pipeline's final "Persist + Emit Event" stage is this task's responsibility to get mode-agnostic from day one, even though task 18 is where the real streaming consumer path is built).
- plans/docs/06-streaming-architecture.md §7.1 (topic table: `ingestion.raw.<shard>` partition key `tenant_id`, 7-day retention — this task's event-emission stage targets this topic name/partitioning even at MVP, using the local Redpanda from task 02) and §7.3 (transactional outbox pattern — the "Persist + Emit Event" stage should write the persisted row and the emitted event in the same Postgres transaction via an outbox table, not a dual write; the actual Kafka producer relay reading the outbox can be a simple synchronous best-effort dispatch at MVP scale, since full Debezium-outbox-connector wiring is V1/task 18 — see Non-Goals).
- plans/docs/07-api-extensibility.md §8.3 (Connector SDK / WASM sandboxing — this is V1/task 25; this task's registry and interface must be shaped so a WASM-backed connector could implement `SourceConnector` later without an interface change, but this task itself only registers native Go connectors).
- plans/docs/16-development-workflow.md §16.1 (`internal/ingestion/connector/` — interface+registry; `internal/ingestion/parsers/` — format-specific parsers, populated by task 07; `internal/ingestion/mapping/` — field-mapping DSL engine, task 08).

## Implementation Notes
- `SourceConnector` interface — implement exactly as specified in plans/docs/02-data-ingestion.md §3.1:
  ```go
  type SourceConnector interface {
      Fetch(ctx context.Context, cfg ConnectorConfig) (<-chan RawRecord, error)
      Validate(cfg ConnectorConfig) error
      SupportsStreaming() bool
      Checkpoint() (Cursor, error)
  }
  ```
  Supporting types:
  ```go
  type RawRecord struct {
      TenantID     uuid.UUID
      ConnectorID  uuid.UUID
      SourceFormat string          // "MT940", "CSV", "SFTP_CSV", etc. — matches task 08's mapping DSL source_format
      Payload      []byte          // raw bytes as received, before any parsing
      ReceivedAt   time.Time
      BatchID      uuid.UUID       // groups records from one fetch/file/poll cycle — becomes Statement grouping upstream
      SourceMode   SourceMode      // BATCH | STREAM, see task 05's domain enum — reused, not redefined
  }

  type ConnectorConfig struct {
      ConnectorID uuid.UUID
      TenantID    uuid.UUID
      Type        string          // registry lookup key
      Settings    json.RawMessage // connector-specific config (paths, hostnames, schedule) — secrets are Vault path refs within this, never inline, per §16.3
      Schedule    string          // cron expression, empty for pure-streaming connectors
  }

  type Cursor struct {
      ConnectorID uuid.UUID
      State       json.RawMessage // opaque watermark/offset, connector-specific shape
      UpdatedAt   time.Time
  }
  ```
  `Fetch` returns a channel so the pipeline can consume records as they arrive rather than buffering a whole file/batch in memory — this is required for the CSV/Excel connector's "no full-file memory load" streaming-parser requirement in task 07, so the channel-based signature must not be weakened to a slice-returning one even though it's more complex to implement.
- Registry (`registry.go`):
  ```go
  type Constructor func(cfg ConnectorConfig) (SourceConnector, error)

  type Registry struct {
      mu   sync.RWMutex
      ctor map[string]Constructor
  }

  func (r *Registry) Register(connectorType string, ctor Constructor)
  func (r *Registry) New(connectorType string, cfg ConnectorConfig) (SourceConnector, error)
  ```
  Registration happens via each connector package's `init()` calling a package-level default registry's `Register`, OR (preferred, more testable) explicit registration in each `cmd/ingestion-gateway/main.go`-style wiring point — pick explicit registration (avoids hidden `init()`-order coupling, consistent with §16.3's manual-constructor-injection philosophy) and document the choice.
- Pipeline stages (`pipeline/stage.go`), one `Stage` interface, implementations for each of the 8 named stages in §3.2, composed by `pipeline.go`'s `Run`:
  ```go
  type PipelineRecord struct {
      Raw          RawRecord
      ParsedFields map[string]any     // after Format Parse
      MappedFields map[string]any     // after Field Mapping (task 08's DSL output)
      Normalized   NormalizedFields   // after Normalization (task 08) — typed struct, not map, once shape is known
      Errors       []StageError       // accumulated non-fatal issues; a fatal error short-circuits to quarantine
  }

  type Stage interface {
      Name() string
      Process(ctx context.Context, rec *PipelineRecord) (StageResult, error)
  }

  type StageResult int
  const (
      StageContinue StageResult = iota
      StageQuarantine            // record is invalid, route to quarantine.go's sink, stop processing THIS record only
      StageDrop                  // deliberate skip (e.g. duplicate under tenant's "reject resend" policy) — must be logged, never silent
  )
  ```
  Stage 1 (Raw Fetch) and stage 2 (Format Parse) are largely connector/parser-owned (task 07 supplies the parse logic per format); this task's `pipeline.go` is responsible for stages 3-8 orchestration and defines the seams (interfaces) stages 1-2 plug into. Field Mapping (stage 4) and Normalization (stage 5) interfaces are defined here but their DSL implementation is task 08; Dedup (stage 6) interface is defined here but implementation is task 09; Validation (stage 5, business+schema) interface defined here, implementation task 09.
  **Crucial ordering note**: §3.2 lists the stages as `Raw Fetch → Format Parse → Field Mapping → Normalization → Validation → Dedup/Idempotency → Canonicalization → Persist + Emit Event` — implement in exactly this order (validation before dedup, not after) since a duplicate-but-invalid record should be quarantined for its validation failure, not silently deduped away, giving clearer error attribution.
- Quarantine sink (`quarantine.go`):
  ```go
  type QuarantineEntry struct {
      ID           uuid.UUID
      TenantID     uuid.UUID
      ConnectorID  uuid.UUID
      Stage        string
      Reason       string
      RawPayload   []byte
      OccurredAt   time.Time
  }

  type QuarantineSink interface {
      Quarantine(ctx context.Context, tenantID uuid.UUID, entry QuarantineEntry) error
      List(ctx context.Context, tenantID uuid.UUID, connectorID uuid.UUID) ([]QuarantineEntry, error)
  }
  ```
  A Postgres-backed default implementation needs a `quarantine_entries` table — since task 03's migration didn't include this table (it's implied by §3.3 but not listed in the §4.1 entity list), add it via a new migration file in this task (`migrations/0002_quarantine_entries.up/.down.sql`), following task 03's expand-contract convention. Document this as a deliberate addition beyond the original entity list, justified by §3.3's explicit requirement ("failures land in quarantine queue... never silently drop financial data").
- Event emission (final stage): write the persisted `Transaction`/`Statement` row and an outbox row (`ingestion_outbox` table — another small addition via migration, same justification as quarantine_entries, needed to implement §7.3's transactional-outbox pattern) in the same DB transaction (using task 05's `WithTx` helper); a simple relay goroutine (or a small poller in `cmd/ingestion-gateway`) reads unsent outbox rows and publishes them to the local Redpanda `ingestion.raw.<tenant_shard>` topic, marking them sent. This is a deliberately simplified MVP version of the outbox pattern — full Debezium-based CDC relay is task 18/22.
- Concurrency: the pipeline must process records from a connector's `Fetch` channel concurrently up to a bounded worker pool (size configurable, default small, e.g. `runtime.GOMAXPROCS(0) * 2`) — avoid unbounded goroutine-per-record spawning; use a worker-pool or `golang.org/x/sync/errgroup` with a semaphore.

## Non-Goals / Guardrails
- Do not implement any concrete connector (CSV, SFTP, MT940, etc.) — that is task 07. This task only defines the interface, registry, and a trivial fake connector used solely for this task's own tests.
- Do not implement the field-mapping DSL parser/transform functions — task 08. This task defines the `Stage` interface seam field mapping plugs into, not the DSL engine itself.
- Do not implement dedup/idempotency-key computation or the Redis bloom filter — task 09. This task defines the `Stage` seam, not the algorithm.
- Do not implement real business/schema validation rules — task 09. Define the seam only.
- Do not build BAI2, ISO20022, REST/API pull, Kafka-topic-ingestion, or webhook-receiver connectors — all explicitly deferred to V1 per plans/docs/11-scalability-roadmap.md §12.2 Phase 0 scope (MVP is CSV/Excel, SFTP, one MT940 parser only, built in task 07).
- Do not implement WASM sandboxing or the third-party Connector SDK scaffold CLI — V1/task 25. This task's interface should merely not preclude it later.
- Do not implement full Debezium/Kafka-Connect-based outbox relay — the simplified in-process relay described above is sufficient for MVP; full CDC-based relay is task 18/22.
- Do not implement the streaming matching consumer or any matching logic — tasks 10-12/19.

## Definition of Done
- Unit tests: registry register/lookup/duplicate-registration-rejected; pipeline runs a fake connector's records through all 8 stages in order (verified via stage-name assertion order, not just final output) and reaches the final persist+emit stage for valid records.
- A test proves a record that a stage marks `StageQuarantine` is routed to the `QuarantineSink` and does NOT continue to later stages, while sibling records in the same batch continue processing unaffected (per plans/docs/15-end-to-end-flows.md §15.5's "one bad record never halts the pipeline" principle).
- Integration test (`testcontainers-go` Postgres + local Redpanda): a fake connector's records flow through the full pipeline, end with a `Transaction`/`Statement` row persisted (via task 05 repositories) and a corresponding event message readable from the `ingestion.raw.<tenant_shard>` topic in the same test — proving the transactional-outbox-to-Kafka relay actually works, not just the DB write half.
- Manual verification: running the fake test connector through `cmd/ingestion-gateway` (a minimal wiring added for this task's verification, real CLI/scheduling wiring can stay thin until task 07 needs it) against the local dev stack produces a row in `transactions` and a message on Redpanda visible via `rpk topic consume`.

## Common Pitfalls
- Redesigning the `SourceConnector` interface signature (e.g. adding extra methods, changing `Fetch`'s return type to a slice instead of a channel) "for convenience" — the interface in plans/docs/02-data-ingestion.md §3.1 is deliberately minimal and is the documented extensibility contract; deviating breaks task 07's connectors and the future WASM SDK compatibility goal.
- Buffering an entire file/batch into memory inside `Fetch` before sending anything on the channel — defeats the purpose of the channel-based signature and directly contradicts task 07's CSV/Excel "no full-file memory load" requirement, which depends on this task's streaming-friendly seam being real, not just typed as a channel while actually blocking until fully buffered.
- Implementing validation (stage 5) before dedup (stage 6) out of order, or swapping their order "since dedup seems more fundamental" — the specified order in §3.2 is deliberate (validate first, catches malformed duplicates with a clearer error) and later tasks (09) assume this ordering.
- Building real BAI2/ISO20022/webhook connector stubs now because the interface makes it tempting — strictly out of scope; Phase 0 is CSV/SFTP/MT940 only (task 07).
- Skipping the transactional-outbox pattern and instead publishing to Redpanda directly inside the pipeline stage before/without a DB commit — this reintroduces the dual-write problem §7.3 explicitly designs the outbox pattern to avoid (a crash between DB write and Kafka publish would either lose the event or double-process it).
- Treating the quarantine sink as optional/log-only — plans/docs/02-data-ingestion.md §3.3 is explicit that failures must be durably surfaced for manual remediation, never silently dropped; a log line is not sufficient, it must be a queryable persisted record.
