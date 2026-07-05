-- plans/task/core/24: Citus distribution. Only takes effect when run
-- against a Citus-enabled cluster (the opt-in `--profile citus`
-- docker-compose stack via scripts/migrate-citus.sh) - the Citus
-- extension functions used here (create_distributed_table,
-- create_reference_table) don't exist on plain Postgres, so this
-- migration is NOT part of the default single-node dev stack's
-- migration path.
--
-- Applied via `psql -f` (scripts/migrate-citus.sh), not golang-migrate:
-- golang-migrate's postgres driver sends a migration file as a single
-- prepared statement, and Postgres rejects "multiple commands" in one
-- PREPARE - found via direct testing, this file's mix of DDL and
-- Citus's own SELECT function calls hits exactly that limitation.
-- `psql -f` uses the simple query protocol instead, which allows a
-- multi-statement file same as every interactive psql session run
-- while developing this migration. This intentionally sits outside
-- golang-migrate's up/down version tracking (see migrations/citus's
-- own idempotency: every statement uses IF EXISTS/re-runnable guards
-- where practical) - a schema_migrations_citus marker table records
-- that this ran, for scripts/migrate-citus.sh's own bookkeeping only.
--
-- Every statement below was individually verified against a real
-- 2-worker Citus 12 cluster before being assembled here - three real
-- constraints were found this way, not assumed from documentation:
--
-- 1. Citus requires a distributed table's PRIMARY KEY (and any UNIQUE
--    constraint) to include the distribution column. Every table in
--    this schema (tasks 01-23) was created with a bare `id uuid
--    PRIMARY KEY` - ALL of them need widening to (tenant_id, id), not
--    just the obviously-hot ones. Two non-PK UNIQUE constraints
--    (transactions.ingestion_idempotency_key,
--    tenant_api_keys.key_hash) had the same problem and needed the
--    same composite-column treatment.
-- 2. A LOCAL (not-yet-distributed) table cannot have a foreign key to
--    an ALREADY-distributed table ("foreign keys from reference
--    tables and local tables to distributed tables are not
--    supported"). This forces a strict order per table: drop the FKs
--    pointing AT it (so its PK can change), widen its PK, distribute
--    it, THEN re-add composite FKs pointing FROM it to whatever it
--    references (which must already be distributed/reference by that
--    point).
-- 3. A single batch sent as one implicit transaction rolls back
--    entirely if any statement fails - every ALTER below uses
--    IF EXISTS/re-runnable forms where practical so a partial re-run
--    after a fix doesn't error on already-completed steps.
--
-- End result: every relationship in the original schema is preserved
-- as a genuine composite foreign key (tenant_id, x) - no FK was
-- permanently dropped. `tenants` becomes a Citus REFERENCE table
-- (replicated to every node, not sharded - an even stronger form of
-- "unsharded" than a single-node table, and Citus's own recommended
-- pattern for a small control table many distributed tables reference)
-- so those FKs remain enforceable. tenant_settings/
-- tenant_isolation_config/tenant_quota/tenant_feature_flags are picked
-- up by Citus as local-managed tables automatically (via their FK to
-- the now-reference tenants table) and stay coordinator-only, per
-- §2.3's "Tenant Registry DB stays unsharded." schema_migrations
-- (golang-migrate's own bookkeeping) is untouched.

CREATE TABLE IF NOT EXISTS schema_migrations_citus (version bigint PRIMARY KEY, applied_at timestamptz NOT NULL DEFAULT now());

SELECT create_reference_table('tenants');

-- === accounts ===
ALTER TABLE match_rules DROP CONSTRAINT IF EXISTS match_rules_source_account_id_fkey;
ALTER TABLE match_rules DROP CONSTRAINT IF EXISTS match_rules_target_account_id_fkey;
ALTER TABLE statements DROP CONSTRAINT IF EXISTS statements_account_id_fkey;
ALTER TABLE transactions DROP CONSTRAINT IF EXISTS transactions_account_id_fkey;
ALTER TABLE cases DROP CONSTRAINT IF EXISTS cases_account_id_fkey;
ALTER TABLE accounts DROP CONSTRAINT IF EXISTS accounts_pkey;
ALTER TABLE accounts ADD PRIMARY KEY (tenant_id, id);
SELECT create_distributed_table('accounts', 'tenant_id');

-- === transactions (co-located with accounts) ===
ALTER TABLE match_result_lines DROP CONSTRAINT IF EXISTS match_result_lines_transaction_id_fkey;
ALTER TABLE transactions DROP CONSTRAINT IF EXISTS transactions_statement_id_fkey;
ALTER TABLE transactions DROP CONSTRAINT IF EXISTS transactions_ingestion_idempotency_key_key;
ALTER TABLE transactions ADD CONSTRAINT transactions_tenant_idempotency_key_key UNIQUE (tenant_id, ingestion_idempotency_key);
ALTER TABLE transactions DROP CONSTRAINT IF EXISTS transactions_pkey;
ALTER TABLE transactions ADD PRIMARY KEY (tenant_id, id);
SELECT create_distributed_table('transactions', 'tenant_id', colocate_with => 'accounts');
ALTER TABLE transactions ADD CONSTRAINT transactions_account_id_fkey
  FOREIGN KEY (tenant_id, account_id) REFERENCES accounts (tenant_id, id);

-- === statements ===
ALTER TABLE statements DROP CONSTRAINT IF EXISTS statements_source_connector_id_fkey;
ALTER TABLE statements DROP CONSTRAINT IF EXISTS statements_pkey;
ALTER TABLE statements ADD PRIMARY KEY (tenant_id, id);
SELECT create_distributed_table('statements', 'tenant_id');
ALTER TABLE transactions ADD CONSTRAINT transactions_statement_id_fkey
  FOREIGN KEY (tenant_id, statement_id) REFERENCES statements (tenant_id, id);

-- === match_rules ===
ALTER TABLE match_results DROP CONSTRAINT IF EXISTS match_results_rule_id_fkey;
ALTER TABLE match_rules DROP CONSTRAINT IF EXISTS match_rules_pkey;
ALTER TABLE match_rules ADD PRIMARY KEY (tenant_id, id);
SELECT create_distributed_table('match_rules', 'tenant_id');
ALTER TABLE match_rules ADD CONSTRAINT match_rules_source_account_id_fkey
  FOREIGN KEY (tenant_id, source_account_id) REFERENCES accounts (tenant_id, id);
ALTER TABLE match_rules ADD CONSTRAINT match_rules_target_account_id_fkey
  FOREIGN KEY (tenant_id, target_account_id) REFERENCES accounts (tenant_id, id);

-- === match_results ===
ALTER TABLE match_result_lines DROP CONSTRAINT IF EXISTS match_result_lines_match_result_id_fkey;
ALTER TABLE match_results DROP CONSTRAINT IF EXISTS match_results_pkey;
ALTER TABLE match_results ADD PRIMARY KEY (tenant_id, id);
SELECT create_distributed_table('match_results', 'tenant_id');
ALTER TABLE match_results ADD CONSTRAINT match_results_rule_id_fkey
  FOREIGN KEY (tenant_id, rule_id) REFERENCES match_rules (tenant_id, id);

-- === match_result_lines (composite PK widened from (match_result_id,
-- transaction_id) to include tenant_id too) ===
ALTER TABLE match_result_lines DROP CONSTRAINT IF EXISTS match_result_lines_pkey;
ALTER TABLE match_result_lines ADD PRIMARY KEY (tenant_id, match_result_id, transaction_id);
SELECT create_distributed_table('match_result_lines', 'tenant_id');
ALTER TABLE match_result_lines ADD CONSTRAINT match_result_lines_match_result_id_fkey
  FOREIGN KEY (tenant_id, match_result_id) REFERENCES match_results (tenant_id, id);
ALTER TABLE match_result_lines ADD CONSTRAINT match_result_lines_transaction_id_fkey
  FOREIGN KEY (tenant_id, transaction_id) REFERENCES transactions (tenant_id, id);

-- === cases ===
ALTER TABLE case_comments DROP CONSTRAINT IF EXISTS case_comments_case_id_fkey;
ALTER TABLE case_audit_events DROP CONSTRAINT IF EXISTS case_audit_events_case_id_fkey;
ALTER TABLE cases DROP CONSTRAINT IF EXISTS cases_pkey;
ALTER TABLE cases ADD PRIMARY KEY (tenant_id, id);
SELECT create_distributed_table('cases', 'tenant_id');
ALTER TABLE cases ADD CONSTRAINT cases_account_id_fkey
  FOREIGN KEY (tenant_id, account_id) REFERENCES accounts (tenant_id, id);

-- === case_comments / case_audit_events ===
ALTER TABLE case_comments DROP CONSTRAINT IF EXISTS case_comments_pkey;
ALTER TABLE case_comments ADD PRIMARY KEY (tenant_id, id);
SELECT create_distributed_table('case_comments', 'tenant_id');
ALTER TABLE case_comments ADD CONSTRAINT case_comments_case_id_fkey
  FOREIGN KEY (tenant_id, case_id) REFERENCES cases (tenant_id, id);

ALTER TABLE case_audit_events DROP CONSTRAINT IF EXISTS case_audit_events_pkey;
ALTER TABLE case_audit_events ADD PRIMARY KEY (tenant_id, id);
SELECT create_distributed_table('case_audit_events', 'tenant_id');
ALTER TABLE case_audit_events ADD CONSTRAINT case_audit_events_case_id_fkey
  FOREIGN KEY (tenant_id, case_id) REFERENCES cases (tenant_id, id);

-- === connectors ===
ALTER TABLE ingestion_dedup DROP CONSTRAINT IF EXISTS ingestion_dedup_source_connector_id_fkey;
ALTER TABLE quarantine_entries DROP CONSTRAINT IF EXISTS quarantine_entries_connector_id_fkey;
ALTER TABLE connectors DROP CONSTRAINT IF EXISTS connectors_pkey;
ALTER TABLE connectors ADD PRIMARY KEY (tenant_id, id);
SELECT create_distributed_table('connectors', 'tenant_id');
ALTER TABLE statements ADD CONSTRAINT statements_source_connector_id_fkey
  FOREIGN KEY (tenant_id, source_connector_id) REFERENCES connectors (tenant_id, id);

-- === mapping_specs, fx_rates: no cross-table FK beyond tenants ===
ALTER TABLE mapping_specs DROP CONSTRAINT IF EXISTS mapping_specs_pkey;
ALTER TABLE mapping_specs ADD PRIMARY KEY (tenant_id, id);
SELECT create_distributed_table('mapping_specs', 'tenant_id');

ALTER TABLE fx_rates DROP CONSTRAINT IF EXISTS fx_rates_pkey;
ALTER TABLE fx_rates ADD PRIMARY KEY (tenant_id, id);
SELECT create_distributed_table('fx_rates', 'tenant_id');

-- === quarantine_entries / ingestion_dedup (FK to connectors) ===
ALTER TABLE quarantine_entries DROP CONSTRAINT IF EXISTS quarantine_entries_pkey;
ALTER TABLE quarantine_entries ADD PRIMARY KEY (tenant_id, id);
SELECT create_distributed_table('quarantine_entries', 'tenant_id');
ALTER TABLE quarantine_entries ADD CONSTRAINT quarantine_entries_connector_id_fkey
  FOREIGN KEY (tenant_id, connector_id) REFERENCES connectors (tenant_id, id);

ALTER TABLE ingestion_dedup DROP CONSTRAINT IF EXISTS ingestion_dedup_pkey;
ALTER TABLE ingestion_dedup ADD PRIMARY KEY (tenant_id, id);
SELECT create_distributed_table('ingestion_dedup', 'tenant_id');
ALTER TABLE ingestion_dedup ADD CONSTRAINT ingestion_dedup_source_connector_id_fkey
  FOREIGN KEY (tenant_id, source_connector_id) REFERENCES connectors (tenant_id, id);

-- === ingestion_outbox, outbox_event, audit_events, audit_chain_tail:
-- no cross-table FK beyond tenants ===
ALTER TABLE ingestion_outbox DROP CONSTRAINT IF EXISTS ingestion_outbox_pkey;
ALTER TABLE ingestion_outbox ADD PRIMARY KEY (tenant_id, id);
SELECT create_distributed_table('ingestion_outbox', 'tenant_id');

ALTER TABLE outbox_event DROP CONSTRAINT IF EXISTS outbox_event_pkey;
ALTER TABLE outbox_event ADD PRIMARY KEY (tenant_id, id);
SELECT create_distributed_table('outbox_event', 'tenant_id');

ALTER TABLE audit_events DROP CONSTRAINT IF EXISTS audit_events_pkey;
ALTER TABLE audit_events ADD PRIMARY KEY (tenant_id, id);
SELECT create_distributed_table('audit_events', 'tenant_id');

-- audit_chain_tail's PK is already tenant_id alone (one row per
-- tenant) - already satisfies the distribution-column requirement.
SELECT create_distributed_table('audit_chain_tail', 'tenant_id');

-- === webhook_subscriptions / webhook_deliveries ===
ALTER TABLE webhook_deliveries DROP CONSTRAINT IF EXISTS webhook_deliveries_subscription_id_fkey;
ALTER TABLE webhook_subscriptions DROP CONSTRAINT IF EXISTS webhook_subscriptions_pkey;
ALTER TABLE webhook_subscriptions ADD PRIMARY KEY (tenant_id, id);
SELECT create_distributed_table('webhook_subscriptions', 'tenant_id');

ALTER TABLE webhook_deliveries DROP CONSTRAINT IF EXISTS webhook_deliveries_pkey;
ALTER TABLE webhook_deliveries ADD PRIMARY KEY (tenant_id, id);
SELECT create_distributed_table('webhook_deliveries', 'tenant_id');
ALTER TABLE webhook_deliveries ADD CONSTRAINT webhook_deliveries_subscription_id_fkey
  FOREIGN KEY (tenant_id, subscription_id) REFERENCES webhook_subscriptions (tenant_id, id);

-- === idempotency_requests, case_routing_configs: no cross-table FK
-- beyond tenants ===
ALTER TABLE idempotency_requests DROP CONSTRAINT IF EXISTS idempotency_requests_pkey;
ALTER TABLE idempotency_requests ADD PRIMARY KEY (tenant_id, id);
SELECT create_distributed_table('idempotency_requests', 'tenant_id');

ALTER TABLE case_routing_configs DROP CONSTRAINT IF EXISTS case_routing_configs_pkey;
ALTER TABLE case_routing_configs ADD PRIMARY KEY (tenant_id, id);
SELECT create_distributed_table('case_routing_configs', 'tenant_id');

-- === tenant_api_keys (key_hash's own UNIQUE constraint needed the
-- same per-tenant widening transactions.ingestion_idempotency_key
-- needed) ===
ALTER TABLE tenant_api_keys DROP CONSTRAINT IF EXISTS tenant_api_keys_key_hash_key;
ALTER TABLE tenant_api_keys ADD CONSTRAINT tenant_api_keys_tenant_key_hash_key UNIQUE (tenant_id, key_hash);
ALTER TABLE tenant_api_keys DROP CONSTRAINT IF EXISTS tenant_api_keys_pkey;
ALTER TABLE tenant_api_keys ADD PRIMARY KEY (tenant_id, id);
SELECT create_distributed_table('tenant_api_keys', 'tenant_id');

-- Tenant registry tables OTHER than tenants itself (tenant_settings,
-- tenant_isolation_config, tenant_quota, tenant_feature_flags) need no
-- explicit action - Citus auto-registers them as local-managed tables
-- via their existing FK to the now-reference tenants table, and they
-- stay coordinator-only per §2.3. schema_migrations (golang-migrate's
-- own bookkeeping, not tenant data) is untouched.

INSERT INTO schema_migrations_citus (version) VALUES (1) ON CONFLICT DO NOTHING;
