# Task 05: Canonical Domain Models and Repositories

## Goal
Translate the canonical data model (plans/docs/03-canonical-data-model.md) into Go domain structs and a repository interface layer over the schema task 03 created, using the `TenantContext`/tenant-id-required convention task 04 established. This is the data-access backbone every functional module (ingestion, matching, case management, API layer) builds on — getting the struct shapes and repository interface signatures right here means later tasks consume a stable contract instead of each inventing their own data access pattern.

## Prerequisites
Task 03 (database schema must exist to map structs/repositories against). Task 04 (TenantContext and the tenant-id-required repository convention this task's repositories must satisfy).

## Scope / Deliverables
- `internal/domain/` — new package holding plain Go structs for every entity (no DB-specific tags beyond what's needed for the chosen mapping approach): `Tenant` (re-export or alias from `internal/tenancy` — do not duplicate the struct, see Implementation Notes), `Account`, `Statement`, `Transaction`, `MatchRule`, `MatchResult`, `MatchResultLine`, `Case`, `CaseComment`, `CaseAuditEvent`, `AuditEvent`, `Connector`.
- `internal/domain/money.go` — the `Money`/multi-currency value type and normalization helpers.
- `internal/storage/postgres/` — concrete repository implementations: `account_repo.go`, `statement_repo.go`, `transaction_repo.go`, `match_rule_repo.go`, `match_result_repo.go`, `case_repo.go`, `connector_repo.go`, plus `db.go` (connection pool setup, `pgx` wiring) and `tx.go` (transaction helper that also handles the `SET LOCAL app.current_tenant_id` call from task 04's middleware contract, for code paths that open their own transactions outside an HTTP request, e.g. background workers).
- Repository interfaces defined in `internal/domain/repository.go` (interfaces live with the domain types they operate on, per the "Postgres can later be swapped" note — see Design Reference) — implementations satisfy them from `internal/storage/postgres`.
- Unit tests (struct/value-type logic) and integration tests (`testcontainers-go`, per repo method) colocated per package.

## Design Reference
- plans/docs/03-canonical-data-model.md §4.1 (authoritative entity/field list — this task's structs must match 1:1 with task 03's already-implemented columns; do not re-derive the schema, read it back from task 03's migration files as the source of truth for exact column names/types) and §4.2 (multi-currency handling: every `Transaction` carries native `amount`/`currency` plus `base_amount`/`fx_rate_to_base`; rules can match on either — this task's `Transaction` struct and `Money` type must support both fields distinctly, not collapse them).
- plans/docs/00-overview-and-architecture.md §1.3 tech-stack table (OLTP row: "Keep data-access layer behind repository interfaces to preserve migration option" — this is the specific rationale for why repositories are interfaces defined in `internal/domain`, not concrete structs imported directly by callers; the CockroachDB-migration-checkpoint note is why this decision matters, not something this task needs to act on beyond keeping repos interface-based).
- plans/docs/01-multi-tenancy.md §2.2 (every repository method takes an explicit tenant scope — task 04 built the enforcement mechanism; this task is the first real code that must satisfy it).
- plans/docs/16-development-workflow.md §16.1 (`internal/storage/postgres/` is where repository implementations + migration runner live, per the layout).

## Implementation Notes
- **Money/multi-currency type** (`internal/domain/money.go`):
  ```go
  type Money struct {
      Amount   decimal.Decimal // shopspring/decimal, NOT float64 — matches numeric(20,4) column precision exactly
      Currency string          // ISO 4217, 3-char uppercase
  }

  // BaseAmount converts Money to the tenant/account's base currency using a
  // stored FX rate captured at ingestion time (never recomputed later —
  // fx_rate_to_base/fx_rate_date are historical facts, not live lookups).
  type FXConversion struct {
      BaseAmount   decimal.Decimal
      RateToBase   decimal.Decimal
      RateDate     time.Time
  }
  ```
  Deliberate choice: use `github.com/shopspring/decimal` (or equivalent fixed-point decimal library) everywhere money is represented in Go — never `float64` for amounts, matching the `numeric(20,4)` Postgres column type and avoiding float-precision bugs in a financial system. This isn't explicitly spelled out in the design docs' Go-level detail but is the only choice consistent with §4.2's precision requirements and the Protobuf `Money{units, nanos, currency_code}` pattern shown in plans/docs/06-streaming-architecture.md §7.2 (which exists specifically to "avoid float precision issues").
- `Transaction` struct fields map directly to task 03's `transactions` table: `ID uuid.UUID`, `TenantID uuid.UUID`, `AccountID uuid.UUID`, `StatementID *uuid.UUID` (nullable), `ExternalRef string`, `Amount decimal.Decimal`, `Currency string`, `FXRateToBase *decimal.Decimal`, `BaseAmount decimal.Decimal`, `ValueDate time.Time`, `BookingDate time.Time`, `Description string`, `CounterpartyRef string`, `TransactionType string`, `Side TransactionSide` (typed enum: `DEBIT`/`CREDIT`), `SourceMode SourceMode` (`BATCH`/`STREAM`), `IngestionIdempotencyKey string`, `Status TransactionStatus` (`UNMATCHED`/`MATCHED`/`PARTIALLY_MATCHED`/`EXCEPTION`), `RawPayload json.RawMessage`, `CreatedAt`/`UpdatedAt time.Time`. Use typed string-based enums (`type TransactionStatus string; const ( ... )`) rather than bare strings, so illegal states are a compile-time-adjacent mistake (caught by exhaustiveness-style lint) rather than a silent typo.
- Repository interface pattern, e.g. `internal/domain/repository.go`:
  ```go
  type TransactionRepository interface {
      Create(ctx context.Context, tenantID uuid.UUID, tx Transaction) (Transaction, error)
      GetByID(ctx context.Context, tenantID uuid.UUID, id uuid.UUID) (Transaction, error)
      ListUnmatched(ctx context.Context, tenantID uuid.UUID, accountID uuid.UUID, valueDateFrom, valueDateTo time.Time) ([]Transaction, error)
      UpdateStatus(ctx context.Context, tenantID uuid.UUID, id uuid.UUID, status TransactionStatus) error
      BulkInsert(ctx context.Context, tenantID uuid.UUID, txs []Transaction) (int, error) // batch upsert path for ingestion/matching write-back, see plans/docs/04-matching-engine.md §5.2 point 5
      ExistsByIdempotencyKey(ctx context.Context, tenantID uuid.UUID, key string) (bool, error) // supports task 09's dedup path
  }
  ```
  Every method's first non-context parameter is `tenantID uuid.UUID` (explicit, even though callers will typically pull it from `tenancy.MustTenantFromContext(ctx)` one layer up in a service/handler) — this explicit-parameter redundancy is the deliberate, concrete implementation of "the lint rule + defense-in-depth" convention: even if `ctx` were somehow tenant-less, the compiler forces a `tenantID` argument to exist at the call site, and the repository implementation must use *that* parameter (not silently re-derive it from ctx) as the actual filter value, then additionally assert it matches `tenancy.MustTenantFromContext(ctx).TenantID` (defensive equality check) before executing the query — this catches any caller bug where a service layer accidentally threads the wrong tenant's ID through by mistake, independent of the RLS layer.
  Define analogous interfaces for `AccountRepository`, `StatementRepository`, `MatchRuleRepository`, `MatchResultRepository` (covers both `MatchResult` and `MatchResultLine` persistence, since they're always written together transactionally), `CaseRepository` (covers `Case`, `CaseComment`, `CaseAuditEvent` — the three are always read/written in the context of one case), `ConnectorRepository`.
- `db.go`: use `pgx` (`github.com/jackc/pgx/v5`) with `pgxpool.Pool`, not `database/sql` + a generic driver — pgx gives native support for Postgres types (`numeric`→`decimal`-compatible scanning via a custom `pgtype` codec, arrays, jsonb) with less adapter glue. Document this as a deliberate concrete choice since the design docs don't name a specific Go Postgres driver.
- `tx.go`: a `WithTx(ctx context.Context, tenantID uuid.UUID, fn func(ctx context.Context) error) error` helper that begins a transaction, executes `SET LOCAL app.current_tenant_id = $1` (parameterized, not string-concatenated, to avoid injection) using the given `tenantID`, runs `fn`, commits/rolls back. This is the non-HTTP-request counterpart to task 04's middleware — used by background workers/batch jobs (task 12) that don't have an inbound HTTP request to derive the transaction's tenant scope from.
- `BulkInsert` implementation should use `pgx.CopyFrom` (Postgres `COPY`) or a multi-row `INSERT ... VALUES (...), (...), ...`, per plans/docs/04-matching-engine.md §5.5's "batch upsert (Postgres COPY/multi-row), not row-by-row" performance requirement — even though the batch matching engine itself is task 10-12, the repository method's performance characteristics need to be right from this task since it's expensive to retrofit later.
- Concurrency: `pgxpool.Pool` is safe for concurrent use across goroutines; repository methods should not hold package-level mutable state; any in-repository caching (if added at all — not required at this task) must be per-tenant-keyed and never leak across tenant boundaries.

## Non-Goals / Guardrails
- Do not implement the `MatchRule` DSL parser/compiler, the matching engine's scoring logic, or the aggregation solver — this task only persists/reads `MatchRule.rule_spec` as opaque `jsonb`/`json.RawMessage`; interpreting it is task 11.
- Do not implement the Case/Break state machine transitions or Temporal workflow wiring — this task's `CaseRepository` is pure CRUD/read-write; the lifecycle logic is task 13.
- Do not implement the ingestion pipeline, connector framework, or field-mapping DSL — those are tasks 06-09; this task only provides the `TransactionRepository`/`StatementRepository`/`ConnectorRepository` those tasks will call into.
- Do not implement the audit hash-chaining write path — task 14 owns that; this task's `AuditEvent`-related repository (if included) is a plain append/read repo, no hashing logic.
- Do not add a second ORM or query-builder layer on top of pgx (e.g. sqlc, ent, gorm) unless it's used purely as a compile-time SQL-check tool that still produces the same repository interface shape — if in doubt, hand-write the SQL in `pgx`-based repositories to keep the pattern simple and consistent with §16.3's "no heavyweight framework unless justified" philosophy.
- Do not implement Citus-aware query routing — repositories query the single local Postgres instance; sharding-aware routing is task 24.

## Definition of Done
- Unit tests: `Money`/`FXConversion` arithmetic (currency mismatch errors, decimal precision preserved through conversion, no float round-trip anywhere in the code path — a test should assert `decimal.Decimal` is used, e.g. by constructing a value that would lose precision as a float64 and confirming it survives).
- Integration tests (`testcontainers-go` Postgres + task 03's migrations applied): each repository's CRUD methods round-trip correctly; `BulkInsert` on 10k+ synthetic transactions completes and all rows are queryable; `ListUnmatched` returns correct partition-scoped results; a cross-tenant read attempt (repo called with tenant A's ID against data seeded for tenant B) returns empty/error, proving both the app-layer explicit-tenant-id parameter AND the RLS policy (task 03/04) combine correctly — this is the first task where the two independent defense-in-depth layers are both exercised together against real repository code.
- A test proves that calling a repository method with a `tenantID` argument that does not match `tenancy.MustTenantFromContext(ctx).TenantID` is rejected before hitting the DB (the defensive equality check from Implementation Notes).
- The tenancy lint analyzer from task 04 passes cleanly against all new repository code in this task (proving the enforcement mechanism actually works against real production code, not just its own fixtures).
- Manual verification: a small throwaway script/test binary inserts a tenant + account + transaction end-to-end through the repository layer and reads it back with correct decimal precision and typed enum values.

## Common Pitfalls
- Using `float64` anywhere in the `Money`/`Transaction.Amount` path — this is a hard-line mistake in a reconciliation engine; every amount field must be `decimal.Decimal` (or equivalent fixed-point type) end to end.
- Defining repository interfaces in `internal/storage/postgres` instead of `internal/domain` — this inverts the intended dependency direction (domain should not depend on the storage implementation package) and undermines the "Postgres can later be swapped" goal from plans/docs/00-overview-and-architecture.md §1.3.
- Silently deriving `tenantID` only from `ctx` inside repository methods and ignoring the explicit `tenantID` parameter (or omitting the parameter and only using ctx) — both undermine the explicit-parameter enforcement convention task 04 built the lint rule for.
- Re-implementing a second `Tenant` struct in `internal/domain` that duplicates (and can drift from) the one task 04 already defined in `internal/tenancy` — alias/reuse it instead.
- Collapsing `MatchResult` and `MatchResultLine` into a single denormalized struct/table access pattern — keep them as the documented 1-to-N relationship (one result, many lines), since the matching engine (task 10-12) needs to allocate partial amounts per line for many-to-many matches.
- Writing row-by-row inserts for bulk ingestion paths "for simplicity now, optimize later" — retrofitting `COPY`-based bulk insert after callers (task 07+) already depend on a row-by-row method signature is expensive; get the batch method shape right now even if its caller doesn't exist yet.
- Treating `raw_payload`/`rule_spec` jsonb columns as an excuse to skip typed Go structs for everything else — only genuinely-opaque/tenant-configured blobs (raw source payloads, compiled rule ASTs interpreted by a later task) should stay as `json.RawMessage`; all first-class entity fields get real Go types.
