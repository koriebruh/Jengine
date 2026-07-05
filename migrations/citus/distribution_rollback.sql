-- Reverses migrations/citus/distribution.sql: undistributes every
-- table (Citus's undistribute_table()), restores every PK/UNIQUE
-- constraint to its original (non-composite) shape, and restores every
-- FK to its original (non-composite) target. undistribute_table
-- requires dropping FKs pointing AT the table first (same ordering
-- constraint as the up migration, in reverse), so this proceeds in
-- reverse dependency order. A manual ops tool (`psql -f
-- migrations/citus/distribution_rollback.sql`), not wired into any
-- script - rolling back a live distributed schema is an operational
-- decision, not something to automate blindly (plans/docs/11-scalability-roadmap.md
-- §12.1's own framing: rebalance/topology operations are an ops
-- runbook item, not application code).

-- === tenant_api_keys ===
ALTER TABLE tenant_api_keys DROP CONSTRAINT tenant_api_keys_tenant_key_hash_key;
SELECT undistribute_table('tenant_api_keys');
ALTER TABLE tenant_api_keys ADD CONSTRAINT tenant_api_keys_key_hash_key UNIQUE (key_hash);

-- === case_routing_configs, idempotency_requests ===
SELECT undistribute_table('case_routing_configs');
SELECT undistribute_table('idempotency_requests');

-- === webhook_deliveries / webhook_subscriptions ===
ALTER TABLE webhook_deliveries DROP CONSTRAINT webhook_deliveries_subscription_id_fkey;
SELECT undistribute_table('webhook_deliveries');
ALTER TABLE webhook_deliveries DROP CONSTRAINT webhook_deliveries_pkey;
ALTER TABLE webhook_deliveries ADD PRIMARY KEY (id);

SELECT undistribute_table('webhook_subscriptions');
ALTER TABLE webhook_subscriptions DROP CONSTRAINT webhook_subscriptions_pkey;
ALTER TABLE webhook_subscriptions ADD PRIMARY KEY (id);

ALTER TABLE webhook_deliveries ADD CONSTRAINT webhook_deliveries_subscription_id_fkey
  FOREIGN KEY (subscription_id) REFERENCES webhook_subscriptions (id);

-- === audit_chain_tail, audit_events, outbox_event, ingestion_outbox ===
SELECT undistribute_table('audit_chain_tail');

SELECT undistribute_table('audit_events');
ALTER TABLE audit_events DROP CONSTRAINT audit_events_pkey;
ALTER TABLE audit_events ADD PRIMARY KEY (id);

SELECT undistribute_table('outbox_event');
ALTER TABLE outbox_event DROP CONSTRAINT outbox_event_pkey;
ALTER TABLE outbox_event ADD PRIMARY KEY (id);

SELECT undistribute_table('ingestion_outbox');
ALTER TABLE ingestion_outbox DROP CONSTRAINT ingestion_outbox_pkey;
ALTER TABLE ingestion_outbox ADD PRIMARY KEY (id);

-- === ingestion_dedup / quarantine_entries (FK to connectors) ===
ALTER TABLE ingestion_dedup DROP CONSTRAINT ingestion_dedup_source_connector_id_fkey;
SELECT undistribute_table('ingestion_dedup');
ALTER TABLE ingestion_dedup DROP CONSTRAINT ingestion_dedup_pkey;
ALTER TABLE ingestion_dedup ADD PRIMARY KEY (id);

ALTER TABLE quarantine_entries DROP CONSTRAINT quarantine_entries_connector_id_fkey;
SELECT undistribute_table('quarantine_entries');
ALTER TABLE quarantine_entries DROP CONSTRAINT quarantine_entries_pkey;
ALTER TABLE quarantine_entries ADD PRIMARY KEY (id);

-- === fx_rates, mapping_specs ===
SELECT undistribute_table('fx_rates');
ALTER TABLE fx_rates DROP CONSTRAINT fx_rates_pkey;
ALTER TABLE fx_rates ADD PRIMARY KEY (id);

SELECT undistribute_table('mapping_specs');
ALTER TABLE mapping_specs DROP CONSTRAINT mapping_specs_pkey;
ALTER TABLE mapping_specs ADD PRIMARY KEY (id);

-- === connectors ===
ALTER TABLE statements DROP CONSTRAINT statements_source_connector_id_fkey;
SELECT undistribute_table('connectors');
ALTER TABLE connectors DROP CONSTRAINT connectors_pkey;
ALTER TABLE connectors ADD PRIMARY KEY (id);
ALTER TABLE ingestion_dedup ADD CONSTRAINT ingestion_dedup_source_connector_id_fkey
  FOREIGN KEY (source_connector_id) REFERENCES connectors (id);
ALTER TABLE quarantine_entries ADD CONSTRAINT quarantine_entries_connector_id_fkey
  FOREIGN KEY (connector_id) REFERENCES connectors (id);

-- === case_audit_events / case_comments ===
ALTER TABLE case_audit_events DROP CONSTRAINT case_audit_events_case_id_fkey;
SELECT undistribute_table('case_audit_events');
ALTER TABLE case_audit_events DROP CONSTRAINT case_audit_events_pkey;
ALTER TABLE case_audit_events ADD PRIMARY KEY (id);

ALTER TABLE case_comments DROP CONSTRAINT case_comments_case_id_fkey;
SELECT undistribute_table('case_comments');
ALTER TABLE case_comments DROP CONSTRAINT case_comments_pkey;
ALTER TABLE case_comments ADD PRIMARY KEY (id);

-- === cases ===
ALTER TABLE cases DROP CONSTRAINT cases_account_id_fkey;
SELECT undistribute_table('cases');
ALTER TABLE cases DROP CONSTRAINT cases_pkey;
ALTER TABLE cases ADD PRIMARY KEY (id);

ALTER TABLE case_audit_events ADD CONSTRAINT case_audit_events_case_id_fkey
  FOREIGN KEY (case_id) REFERENCES cases (id);
ALTER TABLE case_comments ADD CONSTRAINT case_comments_case_id_fkey
  FOREIGN KEY (case_id) REFERENCES cases (id);

-- === match_result_lines ===
ALTER TABLE match_result_lines DROP CONSTRAINT match_result_lines_match_result_id_fkey;
ALTER TABLE match_result_lines DROP CONSTRAINT match_result_lines_transaction_id_fkey;
SELECT undistribute_table('match_result_lines');
ALTER TABLE match_result_lines DROP CONSTRAINT match_result_lines_pkey;
ALTER TABLE match_result_lines ADD PRIMARY KEY (match_result_id, transaction_id);

-- === match_results ===
ALTER TABLE match_results DROP CONSTRAINT match_results_rule_id_fkey;
SELECT undistribute_table('match_results');
ALTER TABLE match_results DROP CONSTRAINT match_results_pkey;
ALTER TABLE match_results ADD PRIMARY KEY (id);
ALTER TABLE match_result_lines ADD CONSTRAINT match_result_lines_match_result_id_fkey
  FOREIGN KEY (match_result_id) REFERENCES match_results (id);

-- === match_rules ===
ALTER TABLE match_rules DROP CONSTRAINT match_rules_source_account_id_fkey;
ALTER TABLE match_rules DROP CONSTRAINT match_rules_target_account_id_fkey;
SELECT undistribute_table('match_rules');
ALTER TABLE match_rules DROP CONSTRAINT match_rules_pkey;
ALTER TABLE match_rules ADD PRIMARY KEY (id);
ALTER TABLE match_results ADD CONSTRAINT match_results_rule_id_fkey
  FOREIGN KEY (rule_id) REFERENCES match_rules (id);

-- === statements ===
ALTER TABLE transactions DROP CONSTRAINT transactions_statement_id_fkey;
SELECT undistribute_table('statements');
ALTER TABLE statements DROP CONSTRAINT statements_pkey;
ALTER TABLE statements ADD PRIMARY KEY (id);
ALTER TABLE statements ADD CONSTRAINT statements_source_connector_id_fkey
  FOREIGN KEY (source_connector_id) REFERENCES connectors (id);

-- === transactions ===
ALTER TABLE match_result_lines DROP CONSTRAINT match_result_lines_transaction_id_fkey;
ALTER TABLE transactions DROP CONSTRAINT transactions_account_id_fkey;
SELECT undistribute_table('transactions');
ALTER TABLE transactions DROP CONSTRAINT transactions_pkey;
ALTER TABLE transactions ADD PRIMARY KEY (id);
ALTER TABLE transactions DROP CONSTRAINT transactions_tenant_idempotency_key_key;
ALTER TABLE transactions ADD CONSTRAINT transactions_ingestion_idempotency_key_key UNIQUE (ingestion_idempotency_key);
ALTER TABLE transactions ADD CONSTRAINT transactions_statement_id_fkey
  FOREIGN KEY (statement_id) REFERENCES statements (id);
ALTER TABLE match_result_lines ADD CONSTRAINT match_result_lines_transaction_id_fkey
  FOREIGN KEY (transaction_id) REFERENCES transactions (id);

-- === accounts ===
ALTER TABLE cases DROP CONSTRAINT cases_account_id_fkey;
ALTER TABLE match_rules DROP CONSTRAINT match_rules_source_account_id_fkey;
ALTER TABLE match_rules DROP CONSTRAINT match_rules_target_account_id_fkey;
ALTER TABLE statements DROP CONSTRAINT statements_account_id_fkey;
ALTER TABLE transactions DROP CONSTRAINT transactions_account_id_fkey;
SELECT undistribute_table('accounts');
ALTER TABLE accounts DROP CONSTRAINT accounts_pkey;
ALTER TABLE accounts ADD PRIMARY KEY (id);

ALTER TABLE cases ADD CONSTRAINT cases_account_id_fkey FOREIGN KEY (account_id) REFERENCES accounts (id);
ALTER TABLE match_rules ADD CONSTRAINT match_rules_source_account_id_fkey FOREIGN KEY (source_account_id) REFERENCES accounts (id);
ALTER TABLE match_rules ADD CONSTRAINT match_rules_target_account_id_fkey FOREIGN KEY (target_account_id) REFERENCES accounts (id);
ALTER TABLE statements ADD CONSTRAINT statements_account_id_fkey FOREIGN KEY (account_id) REFERENCES accounts (id);
ALTER TABLE transactions ADD CONSTRAINT transactions_account_id_fkey FOREIGN KEY (account_id) REFERENCES accounts (id);

SELECT undistribute_table('tenants');

DROP TABLE IF EXISTS schema_migrations_citus;
