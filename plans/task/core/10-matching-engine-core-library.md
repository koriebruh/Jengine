# Task 10: Matching Engine Core Library

## Goal
Build `internal/matching/core`, the single shared blocking-key + candidate-generation + scoring library used by every matching entrypoint in the system: the MVP batch worker (`cmd/matching-batch`, task 12) today, and the V1 streaming consumer (`cmd/matching-stream`, task 19) later. This is called out in `plans/docs/13-implementation-notes.md` as "the single most important correctness-critical module" in the whole codebase — a scoring or blocking bug here silently misreconciles production financial data in both batch and streaming paths at once. The core design commitment this task exists to protect: match logic must never drift between batch and streaming, which is only possible if there is exactly one implementation, not two that started identical and diverged.

## Prerequisites
- Task 03 (database schema and migrations) — canonical `Transaction`/`MatchRule`/`MatchResult`/`MatchResultLine` field shapes this package's types must mirror.
- Task 05 (canonical domain models and repositories) — this package consumes domain `Transaction` values (or a projection of them); field names/types must line up with what task 05 defines.

## Scope / Deliverables
- `internal/matching/core/engine.go` — the core algorithm: blocking index construction, candidate generation, rule-priority chaining, orchestration of a single partition's match run.
- `internal/matching/core/types.go` — `MatchableRecord`, `BlockingKeyDef`, `ToleranceSpec`, `ScoringFieldDef`, `CompiledRule`, `ScoringFunc`, `ScoringRegistry`, `ScoredCandidate`, `MatchOutcome`.
- `internal/matching/core/grouping.go` — the bounded one-to-many/many-to-one grouping helper (MVP-scope aggregation; see Implementation Notes).
- `internal/matching/core/breaksink.go` — the `BreakSink` interface boundary consumed by batch/streaming workers, implemented outside this package (task 13).
- `internal/matching/core/testdata/` — golden-dataset fixtures directory (populated further by task 17, seeded here with a handful of basic cases so this task's own tests are self-sufficient).
- Do **not** create `internal/matching/rules/` (task 11) or `internal/matching/similarity/` (implemented as part of task 11) — this task only defines the `ScoringFunc` type and `ScoringRegistry` interface that those will satisfy.

## Design Reference
- `plans/docs/04-matching-engine.md` §5.1 for the shape of `keys:`/`scoring:`/`thresholds:`/`aggregation_rules:` that `CompiledRule` must be able to represent (this task does not parse YAML — task 11 does — but `CompiledRule` is the compile *target* both tasks agree on).
- `plans/docs/04-matching-engine.md` §5.2 — the batch algorithm steps (partitioning is task 12's concern, but steps 2 and 4 — inverted-index candidate generation and bounded aggregation — are this task's).
- `plans/docs/04-matching-engine.md` §5.4 — confidence scoring/threshold semantics (`auto_match` vs `suggest` vs unsurfaced).
- `plans/docs/16-development-workflow.md` §16.1 module-boundary rule — read this before writing `breaksink.go`: `matching-batch`/`matching-stream` must never import `internal/cases` directly.
- `plans/docs/11-scalability-roadmap.md` §12.2 Phase 0 — MVP matching scope is one-to-one + "simple one-to-many" only; full many-to-many aggregation is explicitly excluded.

## Implementation Notes

### Core types
```go
type MatchableRecord struct {
    ID              uuid.UUID
    TenantID        uuid.UUID
    AccountID       uuid.UUID
    ValueDate       time.Time
    BaseAmount      decimal.Decimal // mirrors Transaction.base_amount numeric(20,4)
    Currency        string
    Reference       string
    CounterpartyRef string
    Side            string // DEBIT|CREDIT
    Extra           map[string]any // escape hatch for scoring fields not in the fixed set
}

type ToleranceSpec struct {
    Kind     string // "exact" | "date_window" | "numeric"
    Days     int             // date_window
    Absolute decimal.Decimal // numeric
    Percent  float64         // numeric
}

type BlockingKeyDef struct {
    Field     string // e.g. "value_date", "base_amount", "currency"
    Tolerance ToleranceSpec
}

type ScoringFunc func(a, b MatchableRecord, field string) (float64, error) // returns [0,1]

// ScoringRegistry is implemented by internal/matching/rules (task 11), not here.
// Core depends on this interface, never on the concrete registry, keeping core
// free of any dependency on the DSL/parsing package.
type ScoringRegistry interface {
    Lookup(name string) (ScoringFunc, bool)
}

type ScoringFieldDef struct {
    Field         string
    Method        string // registry key, resolved via ScoringRegistry at match time
    Weight        float64
    MinSimilarity float64
}

type CompiledRule struct {
    ID              uuid.UUID
    TenantID        uuid.UUID
    Name            string
    Version         int
    Cardinality     string // ONE_TO_ONE | ONE_TO_MANY | MANY_TO_ONE (MANY_TO_MANY rejected at compile time, see task 11)
    BlockingKeys    []BlockingKeyDef
    ScoringFields   []ScoringFieldDef
    AutoMatchThresh float64
    SuggestThresh   float64
    Priority        int // lower runs first
    MaxGroupSize    int // one-to-many bound; MVP hard cap, see grouping.go
}

type ScoredCandidate struct {
    RuleID      uuid.UUID
    SourceIDs   []uuid.UUID
    TargetIDs   []uuid.UUID
    Score       float64
    FieldScores map[string]float64 // per-field breakdown — feeds the "why didn't this match" transparency differentiator (plans/docs/12) and the match-review UI
}

type MatchOutcome struct {
    AutoMatched []ScoredCandidate
    Suggested   []ScoredCandidate
    Unmatched   []uuid.UUID // residue after every active rule in priority order has run
}
```

### Blocking index / candidate generation (avoiding O(N×M))
`BuildBlockingIndex(records []MatchableRecord, keys []BlockingKeyDef) *BlockingIndex` builds `map[string][]*MatchableRecord` keyed by a composite bucket hash of the blocking fields.

Critical subtlety: **tolerance-bearing keys cannot use a single exact hash.** A `date_window` key with `days: 2` and a `numeric` key with an absolute/percent tolerance must fan out over a small bounded set of bucket hashes per record, not one:
- `date_window`: bucket by calendar day; a record with tolerance `days: N` must be inserted into (or looked up against) `2N+1` day-buckets, not just its own day.
- `numeric` tolerance: bucket by rounding `base_amount` to a bucket width derived from the tolerance (e.g. width = `2 × absolute` when `percent == 0`, or a width derived from `percent` otherwise); look up the record's own bucket plus adjacent buckets to catch values near a bucket boundary.
- `exact` (e.g. `currency`): single bucket, no fan-out.

This keeps candidate generation `O(N × fan_out)` where `fan_out` is small and bounded (a handful of buckets), never `O(N×M)`. Fan-out size must be a documented, tested invariant — an unbounded or exponential fan-out here defeats the entire point of blocking and is the single easiest way to silently reintroduce O(N×M) behavior while looking like an optimization.

`Match(ctx, source, target []MatchableRecord, rules []CompiledRule, registry ScoringRegistry) (MatchOutcome, error)` is the top-level entrypoint and owns **rule-priority chaining**: for each `CompiledRule` in ascending `Priority` order, build a fresh blocking index over the records not yet matched by an earlier rule, generate candidates, score via `registry.Lookup`, classify against `AutoMatchThresh`/`SuggestThresh`, remove matched records from the pool, and continue to the next rule. Records still unmatched after the last rule become `MatchOutcome.Unmatched`. This chaining logic lives here — once, centrally — precisely so `matching-batch` (task 12) and `matching-stream` (task 19) never each reimplement it slightly differently.

### Bounded one-to-many grouping (MVP scope only)
`grouping.go` implements a deliberately simple, bounded helper for the "simple one-to-many" MVP requirement (`plans/docs/11-scalability-roadmap.md` §12.2 Phase 0): given one record on the "single" side and a small candidate set from the same blocking bucket on the "many" side, find a subset (size ≤ `CompiledRule.MaxGroupSize`, MVP default small, e.g. 5) whose summed `BaseAmount` falls within the rule's numeric tolerance of the single side's amount. Use bounded brute-force/combinatorial search over the capped candidate set (candidate sets from blocking are already small — this is not a general subset-sum solver over the full partition). Invariants that must hold and be tested: a transaction is never allocated into more than one group, the returned group never exceeds `MaxGroupSize`, and the search always terminates within the cap.

This is explicitly **not** the full many-to-many aggregation solver described in `plans/docs/04-matching-engine.md` §5.2 point 4 (bounded subset-sum/knapsack DP, `max_group_size: 20`, rounded-amount buckets, partial-grouping fallback for human review) — that solver, and `MANY_TO_MANY` cardinality generally, is out of MVP scope per the roadmap and is deferred (V1/V2, not currently assigned a task number in 10–17; note this gap explicitly rather than silently building a bigger solver than asked for).

### `BreakSink` interface boundary
```go
type OpenBreakParams struct {
    TenantID       uuid.UUID
    AccountID      uuid.UUID
    TransactionIDs []uuid.UUID
    BreakType      string // UNMATCHED|AMOUNT_MISMATCH|TIMING_DIFFERENCE|DUPLICATE|FX_VARIANCE|MISSING_COUNTERPARTY
    AmountAtRisk   decimal.Decimal
    Currency       string
}

type BreakSink interface {
    OpenBreak(ctx context.Context, params OpenBreakParams) error
}
```
This interface is defined here (consumed by `Match`'s caller in `cmd/matching-batch`, not called from inside `Match` itself — `Match` only returns `MatchOutcome.Unmatched`; turning that residue into `Break` rows is the batch worker's job, task 12). The concrete implementation lives in `internal/cases` (task 13) and is dependency-injected in `cmd/matching-batch/main.go`, which is the only place allowed to import both `internal/matching/core` and `internal/cases`. Neither `internal/matching/core` nor `internal/matching/rules` may import `internal/cases`.

### Concurrency
`Match` itself should be safe to call concurrently across independent partitions (no shared mutable state beyond arguments) — the batch worker (task 12) is what parallelizes across partitions, this package just needs to not introduce accidental shared state (e.g. no package-level registry, no global cache) that would make concurrent partition processing unsafe.

## Non-Goals / Guardrails
- No YAML/JSON parsing of rules — that is task 11's `internal/matching/rules/dsl.go`. This task only defines the compiled Go structs.
- No worker pool, job queue, or partitioning logic — that is task 12.
- No `internal/matching/similarity` implementations (Jaro-Winkler, Levenshtein, etc.) — those are built in task 11 alongside the DSL, registered into a registry that satisfies this package's `ScoringRegistry` interface.
- No full many-to-many subset-sum/knapsack DP solver, no `MANY_TO_MANY` cardinality support — deferred, see Implementation Notes.
- No ML-based scoring (explicitly V2 per `plans/docs/04-matching-engine.md` §5.3).
- No Redis rolling-window/candidate-pool logic — that is streaming-specific (task 19, V1).
- Do not import `internal/cases` anywhere in this package.

## Definition of Done
- Unit tests (table-driven, colocated `_test.go`) cover: blocking index construction with each `ToleranceSpec.Kind`, fan-out bucket count is bounded and asserted, rule-priority chaining removes matched records between rules, threshold classification (`auto_match`/`suggest`/unsurfaced) boundary conditions.
- Golden-dataset tests: fixture (source transactions, target transactions, expected outcomes) under `internal/matching/core/testdata/`, run against `Match` — this is the suite `plans/docs/16-development-workflow.md` §16.4 calls the most correctness-critical in the codebase; a handful of representative fixtures must exist and pass before this task is done (task 17 expands this suite further, but it must not start empty).
- Property-style tests for `grouping.go` confirming: no transaction double-allocated across groups, group size never exceeds `MaxGroupSize`, search always terminates.
- `go test -race ./internal/matching/core/...` passes.
- Manual verification: a small `go run`-able example (or a test acting as one) constructing ~50 synthetic records across 2-3 blocking-key configurations and confirming candidate count stays near-linear in input size, not quadratic (sanity check against the O(N×M) regression this task exists to prevent).
- Completion is the test suite passing, not a checklist. Any exploratory QA issues go in root-level `QA_REPORT.md` (create if absent), open items only, deleted when fixed.

## Common Pitfalls
- Implementing blocking keys as single exact-match hashes and bolting tolerance on as a post-filter over the *entire* record set — this silently reintroduces O(N×M) and defeats the reason this module exists. Tolerance must be handled via bucket fan-out at index-build/lookup time.
- Putting the `ScoringRegistry` concrete map in this package "for convenience," creating an accidental dependency from `core` on rule-DSL-specific scoring function names — breaks the intended layering where `core` is DSL-agnostic and reusable by anything.
- Building the full many-to-many DP solver here because it's described in §5.2 — re-read `plans/docs/11-scalability-roadmap.md` §12.2 Phase 0 first; it is explicitly excluded from MVP.
- Having `Match` (or anything in `core`) directly write to Postgres or call into `internal/cases` — all persistence is the caller's (task 12's) job; `core` returns data, it doesn't have side effects on tables it doesn't own.
- Forgetting that `MatchOutcome.Unmatched` residue after rule chaining is not automatically a `Break` — a `SUGGESTED` match is not a break either (it only becomes one if an analyst explicitly rejects it later, per `plans/docs/15-end-to-end-flows.md` §15.1 step 13). Don't have `Match` or its caller eagerly open breaks for `Suggested` candidates.
