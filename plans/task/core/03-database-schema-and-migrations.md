# Task 03: Database Schema and Migrations

## Goal
Define the initial Postgres schema for every core entity in the canonical data model, wire up a migration tool with an enforced expand-contract convention, and establish Row-Level Security (RLS) as the defense-in-depth mechanism for tenant isolation. This is the single foundational data layer every other backend task (tenancy context, domain repositories, ingestion, matching, case management) reads/writes against — getting entity shape, keys, and RLS wrong here propagates errors into every later task.

## Prerequisites
Task 02 (local dev infrastructure — needs a running local Postgres to apply migrations against).

## Scope / Deliverables
- `migrations/0001_init_schema.sql` (or split into several numbered files if the chosen tool prefers per-change files, e.g. `0001_create_tenants.up.sql` / `.down.sql` — see Implementation Notes for tool choice) covering:
  - `tenants`, `tenant_settings`, `tenant_isolation_config`, `tenant_quota`, `tenant_feature_flags` (Tenant Registry tables — see task 04 for the registry's runtime role; this task only creates the tables).
  - `accounts`
  - `statements`
  - `transactions`
  - `match_rules`
  - `match_results`
  - `match_result_lines`
  - `cases` (the `Break/Case` entity — table named `cases` per plans/docs/16-development-workflow.md's `internal/cases` package naming; do not name the table `breaks`, see Common Pitfalls)
  - `case_comments`
  - `case_audit_events`
  - `audit_events`
  - `connectors`
  - `ingestion_dedup` (the durable dedup table referenced by task 09 — create it here since it's core schema, task 09 only consumes it)
- `scripts/migrate.sh` — real implementation (task 02 left this as a stub) invoking the chosen migration tool against `$POSTGRES_*` env vars from `.env.example`.
- `scripts/lint/check_migration_safety.sh` (or equivalent) — the migration-lint CI stage from plans/docs/16-development-workflow.md §16.5 stage 4, flagging non-expand-contract-safe migrations (e.g. bare `DROP COLUMN`, `ALTER COLUMN ... SET NOT NULL` without a prior deprecate step).
- RLS policies for every tenant-scoped table (all of the above except the Tenant Registry tables themselves, which are unsharded and not tenant-owned).
- Makefile `migrate` target updated to call the real `scripts/migrate.sh` (task 02's placeholder becomes real).

## Design Reference
- plans/docs/03-canonical-data-model.md §4.1 (authoritative field list per entity — implement every field listed there; do not invent additional business fields beyond what's needed for FKs/timestamps/soft-delete conventions) and §4.2 (multi-currency: `amount`, `currency`, `fx_rate_to_base`, `base_amount` columns on `transactions`).
- plans/docs/01-multi-tenancy.md §2.1 (Standard tier = shared Citus cluster + RLS; this task builds the Standard-tier-only schema — Citus distribution and Isolated/Dedicated tiers are V1, task 24) and §2.2 (RLS as defense-in-depth beyond app-layer filtering — both layers required, RLS is not a substitute for the app-layer tenant_id check task 04/05 build).
- plans/docs/10-observability-reliability.md §11.5 (expand-contract migration convention: add nullable column → dual-write → backfill → cutover reads → drop old; strict — no single-step breaking migrations at this scale).
- plans/docs/09-security-compliance.md §10.1 (hash-chained `AuditEvent` — this task creates the `audit_events` table with a `hash_chain_prev` column; the hashing logic itself is task 14, not this task).
- plans/docs/04-matching-engine.md §5.5 (DB indexing guidance: composite `(tenant_id, account_id, value_date, base_amount)` index on transactions; partial index on `status='UNMATCHED'`; BRIN on time-series columns — implement these indexes now even though the matching engine itself is a later task, since retrofitting indexes on a large table later is expensive).

## Implementation Notes
- **Migration tool choice**: use `golang-migrate/migrate` (the `migrate` CLI + Go library, SQL-file-based, up/down pairs, widely used, simple, no extra runtime dependency in the app binary). This is a deliberate concrete choice where the design docs left the tool unspecified — document the choice in a comment at the top of `scripts/migrate.sh`. Do not use an ORM-driven migration tool (e.g. GORM auto-migrate) — the design commits to raw SQL migrations (`migrations/*.sql` in §16.1's layout) and a repository-interface pattern (task 05), not an ORM.
- File naming: `migrations/NNNN_description.up.sql` / `migrations/NNNN_description.down.sql`, sequential numeric prefix. First migration: `0001_init_schema.up.sql`.
- All primary keys: `uuid` type, generated via `gen_random_uuid()` (`pgcrypto` extension — enable it in `0001`) except `audit_events.id` which is a ULID (26-char string, time-sortable, generated application-side per plans/docs/03-canonical-data-model.md §4.1 — store as `text` or `char(26)`, not `uuid`, since ULIDs aren't native Postgres UUIDs).
- Every tenant-scoped table has `tenant_id uuid NOT NULL REFERENCES tenants(id)` as its first non-PK column, indexed (`tenant_id` should lead every multi-column index used for tenant-scoped lookups so Citus co-location and RLS predicate pushdown both work well later).
- Concrete column set per table (extend plans/docs/03-canonical-data-model.md §4.1 with standard bookkeeping columns — `created_at timestamptz NOT NULL DEFAULT now()`, `updated_at timestamptz NOT NULL DEFAULT now()` on every mutable table; append-only tables like `case_audit_events`/`audit_events` only need `created_at`/`occurred_at`):
  - `tenants(id, name, isolation_tier text CHECK IN ('STANDARD','ISOLATED','DEDICATED'), region, status text, created_at)`
  - `tenant_settings(tenant_id, key, value jsonb, updated_at)` — key/value config store, PK `(tenant_id, key)`.
  - `tenant_isolation_config(tenant_id PK, shard_id, schema_name nullable, cluster_ref nullable)`
  - `tenant_quota(tenant_id PK, ingestion_rate_limit, matching_job_concurrency, storage_quota_bytes, ...)`
  - `tenant_feature_flags(tenant_id, flag_key, enabled bool, PK (tenant_id, flag_key))`
  - `accounts(id, tenant_id, external_account_ref, account_type text CHECK IN ('BANK','GL','GATEWAY','CASH'), currency char(3), name, metadata jsonb, created_at, updated_at)`
  - `statements(id, tenant_id, account_id FK, source_connector_id FK nullable, format text, received_at, period_start date, period_end date, opening_balance numeric(20,4), closing_balance numeric(20,4), status text CHECK IN ('RECEIVED','PARSED','VALIDATED','RECONCILED'), raw_file_ref text, checksum text, created_at, updated_at)`
  - `transactions(id, tenant_id, account_id FK, statement_id FK nullable, external_ref, amount numeric(20,4), currency char(3), fx_rate_to_base numeric(20,10) nullable, base_amount numeric(20,4), value_date date, booking_date date, description text, counterparty_ref text, transaction_type text, side text CHECK IN ('DEBIT','CREDIT'), source_mode text CHECK IN ('BATCH','STREAM'), ingestion_idempotency_key text UNIQUE, status text CHECK IN ('UNMATCHED','MATCHED','PARTIALLY_MATCHED','EXCEPTION'), raw_payload jsonb, created_at, updated_at)`
  - `match_rules(id, tenant_id, name, version int, status text CHECK IN ('DRAFT','ACTIVE','ARCHIVED'), rule_spec jsonb, match_type text CHECK IN ('EXACT','TOLERANCE','FUZZY','COMPOSITE'), source_account_id, target_account_id, priority int, auto_match_threshold numeric(3,2), created_by, approved_by nullable, effective_from timestamptz nullable, created_at, updated_at)`
  - `match_results(id, tenant_id, rule_id FK, match_type text CHECK IN ('ONE_TO_ONE','ONE_TO_MANY','MANY_TO_ONE','MANY_TO_MANY'), confidence_score numeric(4,3), status text CHECK IN ('AUTO_MATCHED','SUGGESTED','CONFIRMED','REJECTED'), matched_at, matched_by nullable, amount_variance numeric(20,4), date_variance int, created_at)`
  - `match_result_lines(match_result_id FK, transaction_id FK, side text CHECK IN ('SOURCE','TARGET'), allocated_amount numeric(20,4), PRIMARY KEY (match_result_id, transaction_id))`
  - `cases(id, tenant_id, account_id FK, related_transaction_ids uuid[], break_type text CHECK IN (...), root_cause_category text nullable, status text CHECK IN ('OPEN','ASSIGNED','IN_PROGRESS','PENDING_APPROVAL','RESOLVED','WRITTEN_OFF','ESCALATED','REOPENED'), assigned_to nullable, priority text, sla_due_at timestamptz nullable, opened_at, resolved_at nullable, amount_at_risk numeric(20,4), currency char(3), temporal_workflow_id text nullable, created_at, updated_at)` — note `REOPENED` is included even though `plans/docs/03-canonical-data-model.md` §4.1's enum text omits it: §6.1's lifecycle diagram clearly requires it (a resolved/written-off case can be reopened on new evidence), so this migration includes it directly rather than leaving task 13 to patch it in later with a supplementary migration.
  - `case_comments(id, case_id FK, actor, event_type default 'comment', payload jsonb, created_at)` — append-only, no `updated_at`.
  - `case_audit_events(id, case_id FK, actor, event_type, payload jsonb, created_at)` — append-only.
  - `audit_events(id text PRIMARY KEY /* ULID */, tenant_id, actor_id, actor_type, event_type, entity_type, entity_id, before_state jsonb, after_state jsonb, ip_address inet, request_id, occurred_at timestamptz, hash_chain_prev text)`.
  - `connectors(id, tenant_id, type, config jsonb /* secrets are Vault path references, never inline — see task 04/security notes */, schedule text nullable, status text, last_run_at timestamptz nullable, cursor_state jsonb nullable, created_at, updated_at)`.
  - `ingestion_dedup(id, tenant_id, idempotency_key text, source_connector_id, ingestion_batch_id, created_at, UNIQUE (tenant_id, idempotency_key))` — this is the authoritative dedup table task 09 upserts into.
- RLS: for every tenant-scoped table, `ALTER TABLE ... ENABLE ROW LEVEL SECURITY;` plus a policy such as:
  ```sql
  CREATE POLICY tenant_isolation ON transactions
    USING (tenant_id = current_setting('app.current_tenant_id')::uuid);
  ```
  The app-layer connection sets `app.current_tenant_id` per request/transaction (this is wired up in task 04 — this task only creates the policies and documents the session-variable contract they depend on, in a comment at the top of the RLS migration file, so task 04's implementer isn't guessing the variable name). `FORCE ROW LEVEL SECURITY` should also be set so table owners aren't implicitly exempt.
- Indexes (per §5.5 forward-reference): on `transactions`, create `(tenant_id, account_id, value_date, base_amount)` composite btree, a partial index `WHERE status = 'UNMATCHED'`, and consider BRIN on `created_at`/`value_date` for time-range scans at scale. Add analogous `(tenant_id, ...)`-leading indexes on `cases(tenant_id, status)`, `match_results(tenant_id, status)`, `statements(tenant_id, account_id, status)`.
- Expand-contract lint script: a simple grep/heuristic script scanning new `*.up.sql` migration files for banned patterns (`DROP COLUMN`, `DROP TABLE`, `ALTER COLUMN .* SET NOT NULL`) unless a matching `-- expand-contract: deprecate-step-ref <earlier migration number>` comment marker is present, per §11.5's mandated multi-step pattern. Wire into CI stage 4 per §16.5 (task 01 left this stage as a no-op placeholder; this task fills it in for real).

## Non-Goals / Guardrails
- Do not implement Citus distribution (`create_distributed_table`) or multi-node sharding — Phase 0 MVP is "Postgres only" per plans/docs/11-scalability-roadmap.md §12.2 and local dev is explicitly single-node plain Postgres per §16.2. Citus adoption is a V1/scaling-roadmap concern (§12.1), not schema work here.
- Do not implement Isolated-Schema or Dedicated-tier physical isolation (separate schemas/clusters) — this task builds the Standard-tier (shared + RLS) schema only, per plans/docs/01-multi-tenancy.md §2.1 and roadmap Phase 0 ("Basic RLS multi-tenancy (defer full tiered isolation)"). `tenant_isolation_config` table exists as a row-shape placeholder for future tiers, not as working multi-tier logic.
- Do not implement the audit hash-chaining computation logic (the `hash_chain_prev` linking algorithm) — that's task 14. This task only creates the column.
- Do not implement Debezium/CDC wiring — V1 scope (task 18+/22).
- Do not write the application-layer `SET app.current_tenant_id` session-variable-setting code — that is task 04's job. This task only creates RLS policies that assume that contract exists.
- Do not build the repository/Go-struct layer — that's task 05.
- Do not add ClickHouse schema or materialized views — V1 scope (task 22).

## Definition of Done
- `make migrate` (via `scripts/migrate.sh`) applies all migrations cleanly against a fresh local Postgres from `make dev-up`, and `migrate ... down` for each step reverses cleanly (verify down-migrations exist and work, not just up).
- Integration test (`testcontainers-go`, per §16.4) spins up a real Postgres, applies migrations, and asserts: all expected tables/columns exist with correct types; RLS is enabled and `FORCE`d on every tenant-scoped table; a query issued without `app.current_tenant_id` set returns zero rows (or errors) even though rows exist for tenant A when queried as if for tenant B — i.e., the RLS policy actually blocks cross-tenant reads at the DB layer, proven with two seeded tenants and a raw SQL query using `SET app.current_tenant_id`.
- `check_migration_safety.sh` has its own test proving it flags a deliberately-added breaking migration fixture and passes a compliant expand-only one.
- Manual verification: `psql` against the local dev DB after `make migrate` shows the full table list matching this spec; `\d+ transactions` shows the composite and partial indexes.

## Common Pitfalls
- Naming the break/case table `breaks` instead of `cases` — plans/docs/16-development-workflow.md §16.1 names the Go package `internal/cases`, and plans/docs/05-case-management.md consistently calls the workflow entity "Break/Case" interchangeably but the operative table backing the case workflow should be named to match the package it belongs to (`cases`) for consistency across the codebase; picking `breaks` here creates a naming mismatch every later task (13+) has to work around.
- Forgetting `FORCE ROW LEVEL SECURITY` — without it, the table owner role (often the same role the app connects as, if not carefully separated) bypasses RLS silently, defeating the defense-in-depth purpose entirely.
- Treating RLS as sufficient on its own and skipping the app-layer explicit `tenant_id` filter convention — the design is explicit that RLS is defense-in-depth *in addition to* app-layer filtering (§2.2), not a replacement. Don't let this task's existence become a reason task 04/05 skip explicit tenant_id parameters.
- Adding `ON DELETE CASCADE` indiscriminately on FKs touching `audit_events`/`case_audit_events` — these are append-only/immutable by design; cascading deletes from a parent (e.g. deleting a case) must not silently destroy audit history. Prefer `ON DELETE RESTRICT` or no cascade for anything audit-related.
- Using `serial`/`bigserial` integer PKs instead of `uuid`/ULID — the data model is UUID-keyed throughout (except the ULID-keyed `audit_events`); integer PKs would break the documented `id (uuid, PK)` convention everywhere.
- Trying to implement Citus `create_distributed_table` "since it's mentioned in the docs" — that's explicitly deferred; local/MVP schema is plain Postgres tables only.
- Forgetting the `ingestion_dedup` table (easy to overlook since it's introduced narratively in task 02-data-ingestion.md §3.4, not the entity list in §4.1) — task 09 depends on it existing from this task, not creating it itself.
