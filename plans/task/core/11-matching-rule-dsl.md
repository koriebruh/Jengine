# Task 11: Matching Rule DSL

## Goal
Build `internal/matching/rules/dsl.go` (and supporting files), the parser/compiler that turns a tenant-authored YAML/JSON rule spec into the `core.CompiledRule` struct that `internal/matching/core` (task 10) executes. This is the layer that makes matching logic tenant-configurable without code changes — the DSL and its pluggable scoring registry are what let a non-engineer (or the future no-code Rule Builder UI, V1) express "match bank vs GL within 2 days, ±0.01, fuzzy on reference" declaratively. It also implements the concrete `internal/matching/similarity` scoring functions needed for MVP (exact, tolerance, basic Jaro-Winkler) and registers them.

## Prerequisites
- Task 10 (matching engine core library) — this task compiles into `core.CompiledRule`, `core.BlockingKeyDef`, `core.ScoringFieldDef`, and implements `core.ScoringRegistry`/`core.ScoringFunc`.

## Scope / Deliverables
- `internal/matching/rules/dsl.go` — `RuleSpec` (raw parsed shape), `ParseYAML`/`ParseJSON`, `Compile(spec RuleSpec, registry ScoringRegistry) (core.CompiledRule, error)`.
- `internal/matching/rules/registry.go` — `Registry` implementing `core.ScoringRegistry`, `NewRegistry()`, `Register(name string, fn core.ScoringFunc)`, `DefaultRegistry()` pre-populated with MVP built-ins.
- `internal/matching/rules/status.go` — `RuleStatus` type (`DRAFT`/`ACTIVE`/`ARCHIVED`) and the validation rules around status transitions this package is responsible for (see Implementation Notes — this package validates, it does not persist or enforce approval workflow).
- `internal/matching/similarity/jaro_winkler.go`, `internal/matching/similarity/levenshtein.go` (normalized), `internal/matching/similarity/numeric.go` (numeric closeness / tolerance banding), `internal/matching/similarity/date.go` (date proximity) — the concrete similarity implementations backing the registry's built-in `ScoringFunc`s.
- `internal/matching/rules/testdata/` — sample rule YAML fixtures (including the exact example from the design doc) for parser tests.

## Design Reference
- `plans/docs/04-matching-engine.md` §5.1 — the full YAML rule example (`keys:`, `scoring:`, `thresholds:`, `aggregation_rules:`, `execution:`) is the exact shape `RuleSpec` must parse. Open that doc rather than re-deriving the schema from memory.
- `plans/docs/04-matching-engine.md` §5.3 — which similarity techniques are in scope: Jaro-Winkler, normalized Levenshtein/Damerau-Levenshtein, numeric absolute/percent/asymmetric tolerance bands, calendar-aware date windows. MVP builds Jaro-Winkler + numeric + date; normalized Levenshtein is a stretch-if-time within this task but not required for MVP sign-off (see Non-Goals).
- `plans/docs/11-scalability-roadmap.md` §12.2 Phase 0 — MVP rule scope precisely: "exact + tolerance + basic fuzzy (Jaro-Winkler), one-to-one + simple one-to-many." Many-to-many aggregation and ML scoring are explicitly excluded from MVP.
- `plans/docs/03-canonical-data-model.md` §4.1 `MatchRule` entity — `status (DRAFT|ACTIVE|ARCHIVED)`, `rule_spec (jsonb, compiled DSL AST)`, `version`, `approved_by`, `effective_from` — this task's compiled output is what gets stored in `rule_spec`.
- `plans/docs/15-end-to-end-flows.md` §15.3 — the rule authoring/activation flow this DSL package is one piece of (parsing/compiling only; CRUD + maker-checker approval + activation live elsewhere, see Non-Goals).

## Implementation Notes

### RuleSpec (parsed YAML/JSON shape)
Mirror the §5.1 example field-for-field:
```go
type RuleSpec struct {
    Rule struct {
        Name            string          `yaml:"name"`
        Version         int             `yaml:"version"`
        Scope           ScopeSpec       `yaml:"scope"`
        MatchCardinality string         `yaml:"match_cardinality"` // ONE_TO_ONE | ONE_TO_MANY | MANY_TO_ONE | MANY_TO_MANY
        Keys            []KeySpec       `yaml:"keys"`
        Scoring         []ScoringSpec   `yaml:"scoring"`
        Thresholds      ThresholdSpec   `yaml:"thresholds"`
        AggregationRules AggregationSpec `yaml:"aggregation_rules"`
        Execution       ExecutionSpec   `yaml:"execution"`
    } `yaml:"rule"`
}

type KeySpec struct {
    Field     string          `yaml:"field"`
    Tolerance ToleranceYAML   `yaml:"tolerance"` // either the literal string "exact" or a nested object
}

type ScoringSpec struct {
    Field         string  `yaml:"field"`
    Method        string  `yaml:"method"`
    Weight        float64 `yaml:"weight"`
    MinSimilarity float64 `yaml:"min_similarity"`
}

type ThresholdSpec struct {
    AutoMatch float64 `yaml:"auto_match"`
    Suggest   float64 `yaml:"suggest"`
}
```
`ToleranceYAML` needs custom unmarshal handling because the doc's example uses `tolerance: exact` (bare string) for one key and `tolerance: { type: date_window, days: 2 }` (object) for another — implement a custom `UnmarshalYAML` that accepts both shapes.

### Compile
`Compile(spec RuleSpec, registry ScoringRegistry) (core.CompiledRule, error)` steps:
1. Validate `MatchCardinality` is one of `ONE_TO_ONE`, `ONE_TO_MANY`, `MANY_TO_ONE` — **reject `MANY_TO_MANY` at compile time with a clear error** (not silently downgrade it), since the aggregation solver it requires is out of MVP scope (task 10 Non-Goals).
2. Validate `Scoring[].Weight` values sum to a sane total (e.g. warn or normalize if they don't sum to 1.0 — pick one behavior and document it; normalizing is the more forgiving default for tenant-authored YAML).
3. For each `ScoringSpec.Method`, call `registry.Lookup(method)` — **fail compilation immediately** if a referenced method isn't registered. This is the primary defense against a tenant rule silently no-op'ing on a scoring field because of a typo'd method name.
4. Map `KeySpec`/`ScoringSpec`/`ThresholdSpec` into `core.BlockingKeyDef`/`core.ScoringFieldDef`/`AutoMatchThresh`/`SuggestThresh`.
5. `AggregationRules.MaxGroupSize` maps to `core.CompiledRule.MaxGroupSize`, clamped to the MVP bounded-grouping cap from task 10 (reject or clamp values above the MVP cap with a warning — do not silently accept a `max_group_size: 20` from the doc's own example and pass it straight through if task 10's grouping helper cap is smaller; keep the two tasks' caps consistent and documented in one place).
6. `Execution.Mode` (`[batch, streaming]`) is preserved on the compiled rule for the batch/streaming worker to filter on, even though only `batch` matters until task 19 exists.

### Registry
```go
type Registry struct {
    mu    sync.RWMutex
    funcs map[string]core.ScoringFunc
}

func NewRegistry() *Registry
func (r *Registry) Register(name string, fn core.ScoringFunc)
func (r *Registry) Lookup(name string) (core.ScoringFunc, bool)
func DefaultRegistry() *Registry // registers: "exact", "jaro_winkler", "numeric_closeness", "date_proximity"
```
`DefaultRegistry()` is what `cmd/matching-batch` (task 12) wires in. Registration is by string name matching the DSL's `method:` field exactly — `jaro_winkler`, `numeric_closeness`, `date_proximity`, `exact` per the §5.1 example (note the doc example also uses `levenshtein_normalized`; register it too if the similarity package implements it, otherwise the registry must not silently accept rules referencing it — compile-time failure per step 3 above is the correct behavior, not a runtime panic).

### Similarity implementations (MVP scope)
- `jaro_winkler.go`: standard Jaro-Winkler distance, returns similarity in `[0,1]`. Used for short strings with typos near the start (references, names) per §5.3.
- `numeric.go`: `numeric_closeness` — given a tolerance band (absolute and/or percent from the rule spec, resolved at scoring time against the two records' `BaseAmount`), return `1.0` within tolerance, degrading linearly (or some documented curve) outside it up to some cutoff, `0.0` beyond. Support asymmetric tolerance (e.g. GL short by exactly a bank fee) if straightforward; otherwise implement symmetric only and note asymmetric as a follow-up (do not block MVP sign-off on asymmetric tolerance).
- `date.go`: `date_proximity` — similarity based on day-distance within the configured window, `1.0` at zero distance, decaying to `0.0` at the window edge. Calendar-aware business-day/holiday-calendar support (mentioned in §5.3) is **not** required for MVP — plain calendar-day distance is sufficient; note the holiday-calendar refinement as deferred.
- `levenshtein.go` (normalized): optional for this task — build it only if time allows after the required three; do not let it block Definition of Done.

### Status handling
`RuleStatus` (`DRAFT`, `ACTIVE`, `ARCHIVED`) and a small `IsValidTransition(from, to RuleStatus) bool` helper live here purely as a reusable validation primitive. **This package does not persist rules, does not implement the maker-checker approval gate, and does not expose HTTP endpoints** — those are the responsibility of whatever CRUD/API layer creates and activates `MatchRule` rows (task 15 provides the minimal MVP endpoints that call into this package's `ParseYAML`/`Compile`/`IsValidTransition`).

## Non-Goals / Guardrails
- No `MANY_TO_MANY` cardinality support, no full aggregation DP solver (matches task 10's guardrail — reject at compile time, don't half-implement).
- No ML-based scoring function (V2 per §5.3).
- No rule CRUD, no HTTP/API endpoints, no maker-checker approval workflow enforcement, no rule caching/TTL invalidation logic — those belong to task 15 (minimal MVP rule endpoints) and, for the full approval workflow, later work not yet numbered in the MVP set.
- No no-code Rule Builder UI (frontend task 08, V1) — MVP rules are authored as raw YAML/JSON, per `plans/docs/14-dashboard-frontend.md` §14.4.
- No backtesting sandbox (§5.4/§15.3) — that requires re-running historical data through this compiled rule without writing results, which is a larger feature not in the 10–17 task range; note it as a known gap rather than building a partial version.
- Do not import `internal/cases`, `internal/audit`, or any storage/repository package from this task — it is a pure parse/compile/scoring library, same layering discipline as task 10.

## Definition of Done
- Unit tests for `ParseYAML` against the exact §5.1 example (checked into `testdata/`) and at least 2-3 additional fixtures covering edge cases (bare-string vs object `tolerance`, missing optional fields, `MANY_TO_MANY` rejection).
- Unit tests for `Compile` covering: unregistered scoring method fails compilation, weight normalization behavior, `MaxGroupSize` clamping behavior, cardinality validation.
- Unit tests for each similarity function with known input/output pairs (standard Jaro-Winkler test vectors; numeric tolerance boundary cases; date proximity boundary cases).
- Golden-dataset tests (shared with task 10's suite, or a package-local equivalent) confirming a compiled rule end-to-end reproduces expected scores for representative record pairs.
- `go test -race ./internal/matching/rules/... ./internal/matching/similarity/...` passes.
- Manual verification: compiling and running the exact `plans/docs/04-matching-engine.md` §5.1 YAML example against a couple of hand-built `MatchableRecord` pairs produces the expected `AUTO_MATCHED`/`SUGGESTED`/unmatched classification.
- Completion is tests passing; exploratory QA issues go in root-level `QA_REPORT.md`, open items only.

## Common Pitfalls
- Silently accepting an unknown `method:` string at compile time and having it fail (or worse, return `0` similarity) at scoring time deep inside a batch run — must fail fast at `Compile`, with a clear error naming the bad rule and field.
- Accepting `MANY_TO_MANY` and quietly treating it as `MANY_TO_ONE` or `ONE_TO_MANY` — this produces plausible-looking but wrong matches at scale; must be a hard compile error.
- Implementing Jaro-Winkler as plain Jaro (forgetting the Winkler prefix-boost adjustment) — check against known reference test vectors, not just "looks reasonable."
- Letting `Weight` values in `scoring:` go unvalidated/unnormalized such that a rule's total possible score never reaches 1.0, making `auto_match: 0.92` unreachable regardless of match quality — silently produces a rule that can never auto-match, which looks like a matching-engine bug rather than a DSL validation gap.
- Reaching into `internal/cases` or building any persistence/HTTP surface "since it seemed convenient" — this task is scoped to parsing/compiling/scoring only; rule storage and activation belong to task 15.
