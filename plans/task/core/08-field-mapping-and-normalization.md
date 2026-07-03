# Task 08: Field Mapping and Normalization

## Goal
Build the tenant-configurable field-mapping DSL and transform-function registry that implements pipeline stages 4 (Field Mapping) and 5 (Normalization) from task 06's pipeline — the mechanism that lets non-technical ops onboard new source formats or adjust existing ones by editing a versioned YAML/JSON spec, without a code deploy. This is a named competitive differentiator (plans/docs/02-data-ingestion.md §3.2) versus rigid legacy reconciliation tools, and is the layer that absorbs all format differences so the matching engine (task 10+) stays entirely format-agnostic (plans/docs/03-canonical-data-model.md §4.2).

## Prerequisites
Task 06 (ingestion connector framework — this task implements the `Stage` interface seam task 06 defined for Field Mapping/Normalization). Task 07 (concrete connectors — specifically the MT940 parser's `field_61`/`field_86` output shape is what this task's example mapping spec targets; sequPence-wise this task can be developed in parallel with 07's later details but must agree on the field-name contract documented in task 07).

## Scope / Deliverables
- `internal/ingestion/mapping/dsl.go` — mapping spec types (`MappingSpec`, `FieldMapping`) and YAML/JSON (de)serialization.
- `internal/ingestion/mapping/transform.go` — transform function registry (`map[string]TransformFunc`) and the built-in transform set (date parse, currency normalize, sign-flip, trim/case, regex extract, decimal parse).
- `internal/ingestion/mapping/engine.go` — the mapping engine that applies a tenant's versioned `MappingSpec` to a parsed record, implementing task 06's `Stage` interface for both the Field Mapping and Normalization pipeline stages.
- `internal/ingestion/mapping/normalize.go` — currency/date/sign normalization helpers used both as standalone transform functions and as the final Normalization-stage pass that produces the typed `NormalizedFields` struct task 06's `PipelineRecord` expects.
- A `mapping_specs` table + repository (new migration, following task 03's expand-contract convention) to store tenant-configured, versioned mapping specs — referenced by `Connector.config` or `Statement.source_connector_id`.
- Fixture mapping specs for the MT940 and CSV connectors from task 07 (`internal/ingestion/mapping/testdata/mt940_default.yaml`, `csv_default.yaml`).

## Design Reference
- plans/docs/02-data-ingestion.md §3.2 (the authoritative mapping DSL shape — implement the exact YAML structure shown: `source_format`, `mappings[]` with `target`/`source`/`transform[]`; transform chains are ordered lists of named functions, some parameterized e.g. `parse_date("YYMMDD")`, `apply_sign_from(field_61.debit_credit_mark)`, `extract_regex("REF:(\\S+)")` — implement transform-function parameterization exactly this way, not as a different config shape).
- plans/docs/03-canonical-data-model.md §4.2 (multi-currency normalization: every transaction gets `base_amount`/`fx_rate_to_base` computed at ingestion from a tenant-configured FX source; normalization is "the single place format differences are absorbed" so the matching engine never sees per-format quirks; `raw_payload` retains the original unmapped data for drill-down/audit — this task's normalization stage must preserve the original parsed fields alongside the normalized ones, not discard them).
- plans/docs/16-development-workflow.md §16.1 (`internal/ingestion/mapping/` — field-mapping DSL engine location).
- Task 07 (07-ingestion-mvp-connectors.md) Implementation Notes on the MT940 parser's exact field-name output (`field_61.amount`, `field_61.debit_credit_mark`, `field_61.currency`, `field_61.value_date`, `field_86.narrative`) — this task's fixture mapping spec must consume exactly those names.

## Implementation Notes
- `MappingSpec` type (`dsl.go`):
  ```go
  type MappingSpec struct {
      SourceFormat string          `yaml:"source_format"` // "MT940", "CSV", etc.
      Version      int             `yaml:"version"`
      Mappings     []FieldMapping  `yaml:"mappings"`
  }

  type FieldMapping struct {
      Target    string   `yaml:"target"`    // canonical field path, e.g. "transaction.amount"
      Source    string   `yaml:"source"`    // source field path, e.g. "field_61.amount"
      Transform []string `yaml:"transform"` // ordered list of transform function calls, e.g. ["parse_decimal", "apply_sign_from(field_61.debit_credit_mark)"]
  }
  ```
  Parse `Transform` entries as a function-name plus optional parenthesized argument string (e.g. `parse_date("YYMMDD")` → name `parse_date`, arg `"YYMMDD"`; `apply_sign_from(field_61.debit_credit_mark)` → name `apply_sign_from`, arg is itself a field reference, resolved against the record being mapped, not a literal). Write a small parser for this call-syntax (regex or a minimal hand-written tokenizer is sufficient — do not pull in a full expression-language library for this narrow grammar).
- Transform function registry (`transform.go`):
  ```go
  type TransformContext struct {
      Record map[string]any // the parsed source record, for transforms like apply_sign_from that reference sibling fields
  }

  type TransformFunc func(ctx TransformContext, value any, args ...string) (any, error)

  var DefaultRegistry = map[string]TransformFunc{
      "parse_decimal":      parseDecimal,
      "apply_sign_from":    applySignFrom,
      "uppercase":          uppercase,
      "trim":               trim,
      "iso4217_validate":   iso4217Validate,
      "parse_date":         parseDate,        // takes a layout string arg, e.g. "YYMMDD" -> converted to Go reference-time layout internally
      "extract_regex":      extractRegex,     // takes a regex-with-capture-group arg, returns first capture group
  }
  ```
  Registry must be extensible (a `Register(name string, fn TransformFunc)` function) so task-09-level validation transforms or future format-specific transforms can be added without modifying this file — but do not add transforms beyond what's needed for the MT940/CSV fixtures and the explicit list in §3.2's example; resist speculative transform functions.
  `parse_date("YYMMDD")`: implement a small custom-layout-token translator (`YYMMDD`→Go's `060102`, `YYYY-MM-DD`→`2006-01-02`, etc.) rather than requiring tenants to know Go's reference-date layout syntax — tenant-facing config should use a familiar token vocabulary (YYYY/MM/DD/HH/mm/ss), translated internally to Go time layouts. This is a deliberate concrete choice since the docs show `"YYMMDD"` literally and Go's native layout for that would be an unfamiliar `"060102"` string to a non-Go-literate ops user authoring YAML.
  `apply_sign_from(field)`: reads the named sibling field from `TransformContext.Record`, interprets a `D`/`C` (or similar tenant-configurable debit/credit marker set) to flip the sign of `value` (a decimal), returns signed decimal.
- Engine (`engine.go`): loads a tenant's active `MappingSpec` (via the new `mapping_specs` repository, tenant + `source_format`-scoped, versioned — mirrors `MatchRule`'s DRAFT/ACTIVE/ARCHIVED-style versioning convention from task 05's domain model, reused here for consistency even though mapping specs aren't explicitly given that lifecycle in the docs — a deliberate consistent choice), applies each `FieldMapping` in order against the stage-3-parsed record, running the transform chain left-to-right, writing results into the canonical field path. Implements task 06's `Stage` interface:
  ```go
  func (e *MappingEngine) Process(ctx context.Context, rec *pipeline.PipelineRecord) (pipeline.StageResult, error)
  ```
  A transform error (e.g. unparseable date, invalid currency code) marks the field mapping as failed for this record → `StageQuarantine` with a reason string identifying the failing target field and transform — this becomes the concrete "reason" surfaced in the quarantine queue task 06 built the sink for.
- Normalization (`normalize.go`): after field mapping produces raw target values, this pass:
  1. Currency: validate against ISO 4217 (reuse `iso4217_validate` transform or call the same underlying validator), uppercase.
  2. Sign: ensure amount sign convention is consistent (post `apply_sign_from`, amounts should already be correctly signed — this pass is a final sanity assertion, not a second sign-flip).
  3. FX/base-amount: given the mapped `currency` and `amount`, and the account's base currency (looked up via task 05's `AccountRepository`), compute `base_amount`/`fx_rate_to_base` using a tenant-configured FX source — for MVP, implement this as a simple rate-table lookup (a small `fx_rates` reference table, tenant-agnostic or tenant-scoped — add via migration if not already covered by task 03; a full integrated live-FX-provider connector is out of scope, a static/periodically-updated rate table is sufficient and matches plans/docs/03-canonical-data-model.md §4.2's "rate table or integrated provider connector" alternative explicitly allowing the simpler option). If `currency == account.base_currency`, `base_amount = amount`, `fx_rate_to_base = 1`.
  4. Produces the typed `NormalizedFields` struct (matching task 05's `Transaction` field shape closely enough that the Canonicalization stage — task 06's stage 7 — can construct a `Transaction` domain struct directly from it).
- Concurrency: `MappingEngine`/transform functions must be stateless/pure aside from the loaded `MappingSpec` (safe for concurrent use across the pipeline's bounded worker pool from task 06); the mapping-spec lookup should be cached per tenant+source_format with a short TTL (mirrors the rule-cache-with-short-TTL pattern plans/docs/15-end-to-end-flows.md §15.3 describes for `MatchRule`s — reused consistently here).

## Non-Goals / Guardrails
- Do not build a UI/rule-builder for authoring mapping specs — Phase 0 is raw YAML/JSON authoring only (plans/docs/11-scalability-roadmap.md §12.2 explicitly states "rules authored as raw YAML/JSON" for MVP; mapping specs follow the same philosophy — no visual editor).
- Do not implement dedup/idempotency-key computation or business/schema validation rules — those are task 09, even though this task's Normalization stage sits directly before them in the pipeline; keep the boundary at "produce normalized, well-typed fields," not "decide whether the record is valid enough to persist."
- Do not implement a live/integrated FX-rate provider connector — a static rate-table lookup is sufficient for MVP per the explicit alternative the design doc allows; do not build outbound API integration to a live FX service here.
- Do not build a general-purpose expression language/scripting engine for transforms — the transform grammar is deliberately narrow (named function + optional single string/field-reference argument); do not add arbitrary expression evaluation, conditionals, or loops to the DSL.
- Do not implement per-bank MT940 dialect logic itself — that's task 07's parser responsibility; this task only maps whatever field names the parser exposes into canonical fields.
- Do not add mapping-spec approval/maker-checker workflow — that pattern is reserved for `MatchRule` activation (plans/docs/15-end-to-end-flows.md §15.3); mapping specs at MVP can be simpler (versioned, but activation doesn't need a second-approver gate — note this as a deliberate scope reduction versus rule activation, since mapping-spec changes are lower blast-radius than match-rule changes but still versioned for audit/rollback).

## Definition of Done
- Unit tests: each built-in transform function (`parse_decimal`, `apply_sign_from`, `uppercase`, `trim`, `iso4217_validate`, `parse_date`, `extract_regex`) tested with valid and invalid inputs, including the exact examples from plans/docs/02-data-ingestion.md §3.2's YAML (`parse_date("YYMMDD")` against a real 6-digit date string, `extract_regex("REF:(\\S+)")` against a narrative string containing `REF:ABC123`).
- Unit test: the full example YAML from §3.2 parses into a `MappingSpec` and, when applied against a synthetic MT940-parsed record (matching task 07's `field_61`/`field_86` shape), produces correct `transaction.amount` (correctly signed), `transaction.currency`, `transaction.value_date`, `transaction.reference` values.
- Unit test: a transform failure (e.g. malformed date) results in `StageQuarantine` with a reason string naming the failing target field.
- Integration test: FX normalization produces correct `base_amount`/`fx_rate_to_base` for a same-currency transaction (rate=1) and a cross-currency transaction against a seeded `fx_rates` table entry.
- Manual verification: running task 07's MT940 fixture file through the full pipeline (task 06) using this task's fixture mapping spec produces `Transaction` rows with correctly mapped, signed, and currency-normalized fields, verified by inspecting the resulting Postgres rows.

## Common Pitfalls
- Inventing a different mapping-spec YAML shape (e.g. nested differently, or using a different key name than `mappings`/`target`/`source`/`transform`) instead of matching plans/docs/02-data-ingestion.md §3.2's example exactly — tenants and other tasks (07's fixtures) depend on this exact shape.
- Requiring tenants to write Go time-layout strings (`"060102"`) instead of a familiar token vocabulary (`"YYMMDD"`) in `parse_date` — defeats the stated goal of letting "non-technical ops onboard new source formats" (§3.2).
- Discarding the original parsed/raw fields once normalization completes — plans/docs/03-canonical-data-model.md §4.2 requires `raw_payload` to retain original unmapped data for drill-down/audit; the normalization stage must pass both normalized AND original fields forward to the Canonicalization/Persist stage.
- Building a live FX-provider integration or an elaborate scripting engine for transforms — both are explicit scope overreach for this task; a static rate table and a narrow named-function-chain DSL are the correct, deliberately limited scope.
- Applying sign-flipping logic twice (once in `apply_sign_from`, again in the Normalization pass) — the normalization pass should only assert/validate sign consistency, not reapply a second flip, which would silently corrupt amounts.
- Coupling this task's engine tightly to only the MT940 field names — the engine itself must be format-agnostic (it interprets whatever `source_format`-scoped `MappingSpec` is loaded); only the *fixture specs* are MT940/CSV-specific, not the engine code.
