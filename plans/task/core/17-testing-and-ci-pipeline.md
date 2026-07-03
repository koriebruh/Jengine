# Task 17: Testing and CI Pipeline

## Goal
Build the testing harness and CI pipeline that every other MVP task's Definition of Done depends on: the `testcontainers-go` integration-test scaffolding (real Postgres/Redis, not mocks), the golden-dataset test fixtures-and-runner convention for the matching engine, and the CI pipeline stages in the exact order specified by `plans/docs/16-development-workflow.md` §16.4-16.5. This task is infrastructure for every other task, not a feature in its own right — task 10's golden-dataset tests, task 12's integration tests, task 13's integration tests, task 14's concurrency/tamper tests, and task 15's integration tests all assume this harness exists and works. **Despite being numbered last in the human-readable MVP list, this task should be started early, ideally in parallel with task 01, not after task 16** — every other task's "Definition of Done" section references testcontainers-go and the CI stage list defined here, and blocking on task 17 until task 16 is done would leave 16 tasks unable to actually verify completion.

## Prerequisites
None. This is the one MVP core task deliberately designed to have no hard prerequisite, precisely so it can run in parallel with task 01 rather than waiting in the numbered sequence.

## Scope / Deliverables
- `internal/testutil/containers.go` (or `test/integration/containers.go`) — reusable `testcontainers-go` helpers: `StartPostgres(t) *TestDB` (with migrations applied), `StartRedis(t) *TestRedis`, teardown via `t.Cleanup`.
- `internal/matching/core/testdata/README.md` + fixture format convention — the golden-dataset fixture schema (source records, target records, active rules, expected outcomes) and the test runner that loads and executes them (this task defines the *convention and runner*; task 10 populates the initial fixture set and is responsible for keeping it passing).
- `.golangci.yml` — lint configuration, including the custom/grep-based check that every repository query includes an explicit `tenant_id` argument (`plans/docs/01-multi-tenancy.md` §2.2).
- `Makefile` targets: `make test-unit`, `make test-integration`, `make test-golden`, `make lint`, `make ci` (runs the full local equivalent of the CI pipeline).
- CI pipeline definition (e.g. `.github/workflows/ci.yml` or equivalent for whatever CI system core task 01 established) implementing the 6 stages from §16.5 in order.
- `scripts/check-migration-safety.sh` (or a small Go tool) — the migration lint step: flags `DROP COLUMN`/`ALTER COLUMN ... NOT NULL` (or similar breaking changes) without a corresponding prior "deprecate" migration.

## Design Reference
- `plans/docs/16-development-workflow.md` §16.4 — the testing-strategy table this task must set up scaffolding for: unit tests (standard Go table-driven), golden-dataset tests for the matching engine (most correctness-critical suite in the codebase), property-based tests for the aggregation solver, connector conformance tests (framework only — actual connector fixtures belong to ingestion tasks 06/07), integration tests via `testcontainers-go` (real Postgres + Redpanda + Redis — MVP needs Postgres + Redis; Redpanda wiring can be stubbed/prepared but isn't required until task 18/19, V1), and the backtesting sandbox as the pre-production test for rule changes (not this task's to build — it's a product feature, task 11/15's domain, not test infra).
- `plans/docs/16-development-workflow.md` §16.5 — the exact CI stage order: (1) `go vet` + `golangci-lint` including the tenant_id-argument check, (2) `go test -race ./...`, (3) `buf breaking` against the last-published proto schema, (4) migration lint (expand-contract safety), (5) build all `cmd/*` binaries, (6) on merge to main: build/push images, ArgoCD sync. Stage 6 is deploy infrastructure (likely task 01/02's concern for pipeline plumbing) — this task's responsibility is stages 1-5 being correctly defined and enforced; wire stage 6 only if the deploy target already exists, otherwise leave a clearly marked placeholder.
- `plans/docs/10-observability-reliability.md` §11.5 — expand-contract migration discipline is what the migration-lint stage (4) enforces mechanically.
- `plans/docs/04-matching-engine.md` §5.2 point 4 and `plans/docs/16-development-workflow.md` §16.4 — the property-based test requirements for the aggregation solver apply, at MVP, to task 10's *bounded one-to-many grouping helper* (not the full many-to-many DP solver, which doesn't exist yet at MVP) — this task's harness should make writing that property test easy (e.g. a `testing/quick`-style or a small custom generator for synthetic candidate sets), not attempt to test a solver that hasn't been built.

## Implementation Notes

### testcontainers-go harness
```go
type TestDB struct {
    DSN string
    Pool *pgxpool.Pool
}

func StartPostgres(t *testing.T) *TestDB {
    // starts a Postgres testcontainer, applies migrations from migrations/*.sql (task 03),
    // returns a ready-to-use connection pool; registers t.Cleanup to tear down.
}

func StartRedis(t *testing.T) *TestRedis
```
Keep this genuinely reusable — every integration test across tasks 12/13/14/15 should call `StartPostgres(t)` rather than each hand-rolling its own container lifecycle. Apply real migrations (task 03's `migrations/*.sql`), not a hand-maintained test schema copy that can drift from production DDL — the whole point of `testcontainers-go` per §16.4 is testing against real infra behavior, and a schema fork defeats that.

### Golden-dataset fixture convention
Define a fixture format (e.g. YAML or JSON) under `internal/matching/core/testdata/<case_name>/`:
```
testdata/case_exact_match_one_to_one/
  source.json       # []MatchableRecord
  target.json       # []MatchableRecord
  rules.yaml         # one or more rule specs (task 11 RuleSpec shape)
  expected.json      # expected MatchOutcome shape (which pairs auto-matched, suggested, unmatched)
```
The runner (`internal/matching/core/golden_test.go` or similar, in this task) walks `testdata/`, loads each case, compiles the rules (task 11), runs `core.Match` (task 10), and diffs the actual outcome against `expected.json`. This convention and runner are this task's deliverable; task 10 is responsible for populating the initial fixture set (a handful of representative cases) and keeping them green, and any later task that touches blocking/scoring logic must not merge a change that breaks an existing golden fixture without deliberately updating `expected.json` (and that update itself should be a reviewed, visible diff — never a silent regression).

### Lint: tenant_id argument check
`plans/docs/01-multi-tenancy.md` §2.2 requires "repository-layer queries require explicit non-nil `tenant_id`... enforced by lint rule." At MVP, implement this as a `golangci-lint` custom rule if straightforward, or a grep-based script (`scripts/check-tenant-id-arg.sh`) run as an explicit CI step — §16.5 itself says "custom or grep-based," so a simple, maintainable grep/AST-walk over `internal/storage/postgres/*.go` repository method signatures checking for a `tenantID` (or `TenantContext`)-typed parameter is an acceptable MVP implementation; don't over-invest in a full custom `go/analysis` linter if a grep-based check catches the real cases.

### Migration lint
A small script or Go tool that scans new/changed files under `migrations/*.sql` in a PR diff and flags patterns inconsistent with expand-contract (`DROP COLUMN`, `ALTER COLUMN ... SET NOT NULL` without evidence of a prior deprecation migration for the same column, `DROP TABLE` without an prior deprecation marker). This doesn't need to be exhaustively smart — a pattern-matching flag-for-manual-review is sufficient at MVP; false positives that require a human override comment are acceptable, false negatives on truly dangerous migrations are not.

### CI stage wiring
Implement the 6 stages from §16.5 as ordered, fail-fast CI jobs. Stage 3 (`buf breaking`) depends on task 15 having produced the initial `.proto` files and a Buf config — if task 15 isn't done yet when this task is first set up, stage 3 should be wired but tolerant of "no baseline yet" (first-ever proto commit has nothing to break against) rather than failing spuriously; make this explicit in the CI config with a comment, not a silent skip that gets forgotten.

## Non-Goals / Guardrails
- This task does not write the feature-level tests for tasks 10-16's actual business logic — it builds the *harness* (containers, fixture conventions, CI stage wiring). Each of those tasks' own Definition of Done section is what actually populates meaningful test cases using this harness.
- No Kafka/Redpanda testcontainer wiring required for MVP — stub/prepare it if trivial, but it's not needed until task 18/19 (V1); don't block this task's completion on a container type nothing in the MVP scope consumes yet.
- No load testing, chaos testing, or DR game-day tooling — that's V2 (`plans/docs/11-scalability-roadmap.md` §12.2 Phase 2).
- No actual container image build/push or ArgoCD sync implementation if that infrastructure doesn't exist yet from task 01/02 — wire a clearly-marked placeholder stage rather than inventing deploy infra that belongs to a different task's scope.
- No connector-specific conformance fixtures (MT940/BAI2/ISO20022 sample files) — that's tasks 06/07's content; this task only needs to make sure the conformance-test *pattern* (via the Connector SDK test harness, `plans/docs/07-api-extensibility.md` §8.3) has a place to plug into the CI pipeline.
- Do not gold-plate the migration/tenant-id lint checks into full custom static-analysis tools when a grep-based script satisfies the design doc's own stated bar ("custom or grep-based").

## Definition of Done
- `make test-unit`, `make test-integration`, `make test-golden`, `make lint` all runnable locally and passing against an empty/skeleton codebase state (i.e., they work as scaffolding even before tasks 10-16 add substantial content — a trivial passing placeholder test in each category is acceptable proof the harness itself works).
- `StartPostgres`/`StartRedis` helpers demonstrated working via at least one real test using them (can be a minimal smoke test if no other task's tests exist yet at the time this is built).
- The golden-dataset runner correctly loads and diffs at least one hand-written trivial fixture case (even a placeholder), proving the convention and runner work end-to-end before task 10 populates the real set.
- CI pipeline runs all 5 in-scope stages in order on a test PR/commit and fails correctly when a stage's check should fail (verify each stage's negative case: a lint violation is caught, a `-race` failure is caught, a breaking proto change is caught once a baseline exists, an unsafe migration is caught, a compile error in any `cmd/*` is caught).
- `go test -race ./internal/testutil/...` (and any placeholder tests) passes.
- Completion is tests/CI passing, not a checklist. Exploratory QA issues go in root-level `QA_REPORT.md` (create if absent), open items only, deleted when fixed — and note that this convention is exactly what this task exists to make enforceable for every other task, so get it right here.

## Common Pitfalls
- Building this task last (waiting for tasks 10-16 to be "mostly done" first) because it's numbered 17 — this defeats its actual purpose; it must exist early enough that tasks 10-16 can lean on it while being built, not just at the end to validate them.
- Hand-maintaining a separate test-only DB schema instead of applying the real `migrations/*.sql` files inside the testcontainer — schema drift between test and production is exactly the "mock/prod divergence hides real bugs" risk §16.4 calls out for this financial-correctness-critical domain.
- Treating the golden-dataset fixture directory as a one-time seed rather than a living convention — if the runner/format is awkward to extend, later tasks (and task 10 itself) will under-populate it or work around it instead of adding fixtures for every new blocking/scoring behavior.
- Building a full custom static-analysis linter for the tenant_id check when the design doc explicitly says "custom or grep-based" is sufficient — over-investing here trades scarce time away from the actual harness pieces (containers, golden runner) that every other task depends on.
- Wiring stage 6 (image build/push, ArgoCD sync) against infrastructure that doesn't exist yet, causing CI to fail for reasons unrelated to code correctness — mark it as a placeholder clearly if the deploy target isn't ready, rather than blocking every PR on missing infra.
- Forgetting that `buf breaking`'s first run has no baseline to compare against (task 15 is what produces the first `.proto` files) — make sure the CI stage handles "no prior schema" gracefully rather than crashing on every PR until task 15 lands.
