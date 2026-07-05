-- Initial schema per plans/task/core/03 and plans/docs/03-canonical-data-model.md §4.1.
-- Standard-tier (shared Citus cluster + RLS) schema only - see plans/docs/01-multi-tenancy.md
-- §2.1. Citus distribution, Isolated/Dedicated tiers, ClickHouse, and Debezium/CDC are
-- explicitly out of scope here (V1, plans/task/core/18+/22/24).
--
-- RLS session-variable contract (consumed by plans/task/core/04's app-layer code, not
-- built here): every tenant-scoped query must run with
--   SET app.current_tenant_id = '<tenant-uuid>';
-- set for the current transaction/session before querying. RLS policies below compare
-- tenant_id against current_setting('app.current_tenant_id')::uuid. RLS is defense-in-depth
-- IN ADDITION TO the app-layer explicit tenant_id parameter convention
-- (plans/docs/01-multi-tenancy.md §2.2) - it does not replace it.

CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- IMPORTANT: RLS is meaningless if the application connects as a superuser or
-- as a role with BYPASSRLS - both unconditionally bypass RLS regardless of
-- FORCE ROW LEVEL SECURITY. The official postgres Docker image's initial
-- POSTGRES_USER (here: `jengine`) is created as a superuser, so it CANNOT be
-- the role application code (or the RLS enforcement test in
-- plans/task/core/03) connects as. This role is for that purpose - migrations
-- still run as the superuser (needs CREATE TABLE/ROLE privileges), but the
-- application must connect as jengine_app. Password is a fixed local-dev-only
-- value (same pattern as POSTGRES_PASSWORD in .env.example) - never used
-- outside local dev.
-- Idempotent (DO block, not bare CREATE ROLE - Postgres has no CREATE
-- ROLE IF NOT EXISTS): roles are cluster-wide, not per-database/schema,
-- so re-running this migration set against a new Isolated Schema tier
-- tenant's schema (plans/task/core/24 - same Postgres cluster, new
-- schema) would otherwise fail with "role already exists" the second
-- time onward. Found via that task's own schema-provisioning test.
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'jengine_app') THEN
        CREATE ROLE jengine_app LOGIN PASSWORD 'jengine_app_dev'
            NOSUPERUSER NOBYPASSRLS NOCREATEDB NOCREATEROLE;
    END IF;
END
$$;

-- === Tenant Registry (unsharded, not tenant-owned - no RLS on these) =======

CREATE TABLE tenants (
    id              uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    name            text NOT NULL,
    isolation_tier  text NOT NULL CHECK (isolation_tier IN ('STANDARD', 'ISOLATED', 'DEDICATED')),
    region          text NOT NULL,
    status          text NOT NULL,
    created_at      timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE tenant_settings (
    tenant_id   uuid NOT NULL REFERENCES tenants(id),
    key         text NOT NULL,
    value       jsonb NOT NULL,
    updated_at  timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, key)
);

CREATE TABLE tenant_isolation_config (
    tenant_id     uuid PRIMARY KEY REFERENCES tenants(id),
    shard_id      text NOT NULL,
    schema_name   text,
    cluster_ref   text
);

CREATE TABLE tenant_quota (
    tenant_id                   uuid PRIMARY KEY REFERENCES tenants(id),
    ingestion_rate_limit        int NOT NULL,
    matching_job_concurrency    int NOT NULL,
    storage_quota_bytes         bigint NOT NULL
);

CREATE TABLE tenant_feature_flags (
    tenant_id   uuid NOT NULL REFERENCES tenants(id),
    flag_key    text NOT NULL,
    enabled     boolean NOT NULL DEFAULT false,
    PRIMARY KEY (tenant_id, flag_key)
);

-- === Tenant-scoped core entities ============================================

CREATE TABLE accounts (
    id                     uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id              uuid NOT NULL REFERENCES tenants(id),
    external_account_ref   text NOT NULL,
    account_type           text NOT NULL CHECK (account_type IN ('BANK', 'GL', 'GATEWAY', 'CASH')),
    currency               char(3) NOT NULL,
    name                   text NOT NULL,
    metadata               jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at             timestamptz NOT NULL DEFAULT now(),
    updated_at             timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE connectors (
    id                  uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id           uuid NOT NULL REFERENCES tenants(id),
    type                text NOT NULL,
    -- config.secrets are Vault path references, never inline values - see
    -- plans/docs/02-data-ingestion.md §3.1 and plans/task/core/04 security notes.
    config              jsonb NOT NULL DEFAULT '{}'::jsonb,
    schedule            text,
    status              text NOT NULL,
    last_run_at         timestamptz,
    cursor_state        jsonb,
    created_at          timestamptz NOT NULL DEFAULT now(),
    updated_at          timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE statements (
    id                    uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id             uuid NOT NULL REFERENCES tenants(id),
    account_id            uuid NOT NULL REFERENCES accounts(id),
    source_connector_id   uuid REFERENCES connectors(id),
    format                text NOT NULL,
    received_at           timestamptz NOT NULL,
    period_start          date NOT NULL,
    period_end            date NOT NULL,
    opening_balance       numeric(20,4) NOT NULL,
    closing_balance       numeric(20,4) NOT NULL,
    status                text NOT NULL CHECK (status IN ('RECEIVED', 'PARSED', 'VALIDATED', 'RECONCILED')),
    raw_file_ref          text,
    checksum              text,
    created_at            timestamptz NOT NULL DEFAULT now(),
    updated_at            timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE transactions (
    id                          uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id                   uuid NOT NULL REFERENCES tenants(id),
    account_id                  uuid NOT NULL REFERENCES accounts(id),
    statement_id                uuid REFERENCES statements(id),
    external_ref                text,
    amount                      numeric(20,4) NOT NULL,
    currency                    char(3) NOT NULL,
    fx_rate_to_base             numeric(20,10),
    base_amount                 numeric(20,4) NOT NULL,
    value_date                  date NOT NULL,
    booking_date                date,
    description                 text,
    counterparty_ref            text,
    transaction_type            text,
    side                        text NOT NULL CHECK (side IN ('DEBIT', 'CREDIT')),
    source_mode                 text NOT NULL CHECK (source_mode IN ('BATCH', 'STREAM')),
    ingestion_idempotency_key   text NOT NULL UNIQUE,
    status                      text NOT NULL CHECK (status IN ('UNMATCHED', 'MATCHED', 'PARTIALLY_MATCHED', 'EXCEPTION')),
    raw_payload                 jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at                  timestamptz NOT NULL DEFAULT now(),
    updated_at                  timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE match_rules (
    id                      uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id               uuid NOT NULL REFERENCES tenants(id),
    name                    text NOT NULL,
    version                 int NOT NULL,
    status                  text NOT NULL CHECK (status IN ('DRAFT', 'ACTIVE', 'ARCHIVED')),
    rule_spec               jsonb NOT NULL,
    match_type              text NOT NULL CHECK (match_type IN ('EXACT', 'TOLERANCE', 'FUZZY', 'COMPOSITE')),
    source_account_id       uuid REFERENCES accounts(id),
    target_account_id       uuid REFERENCES accounts(id),
    priority                int NOT NULL,
    auto_match_threshold    numeric(3,2) NOT NULL,
    created_by              text NOT NULL,
    approved_by             text,
    effective_from          timestamptz,
    created_at              timestamptz NOT NULL DEFAULT now(),
    updated_at              timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE match_results (
    id                  uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id           uuid NOT NULL REFERENCES tenants(id),
    rule_id             uuid REFERENCES match_rules(id),
    match_type          text NOT NULL CHECK (match_type IN ('ONE_TO_ONE', 'ONE_TO_MANY', 'MANY_TO_ONE', 'MANY_TO_MANY')),
    confidence_score    numeric(4,3) NOT NULL,
    status              text NOT NULL CHECK (status IN ('AUTO_MATCHED', 'SUGGESTED', 'CONFIRMED', 'REJECTED')),
    matched_at          timestamptz NOT NULL DEFAULT now(),
    matched_by          text,
    amount_variance     numeric(20,4),
    date_variance       int,
    created_at          timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE match_result_lines (
    match_result_id     uuid NOT NULL REFERENCES match_results(id),
    transaction_id      uuid NOT NULL REFERENCES transactions(id),
    tenant_id           uuid NOT NULL REFERENCES tenants(id),
    side                text NOT NULL CHECK (side IN ('SOURCE', 'TARGET')),
    allocated_amount    numeric(20,4) NOT NULL,
    PRIMARY KEY (match_result_id, transaction_id)
);

CREATE TABLE cases (
    id                          uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id                   uuid NOT NULL REFERENCES tenants(id),
    account_id                  uuid NOT NULL REFERENCES accounts(id),
    related_transaction_ids     uuid[] NOT NULL DEFAULT '{}',
    break_type                  text NOT NULL CHECK (break_type IN ('UNMATCHED', 'AMOUNT_MISMATCH', 'TIMING_DIFFERENCE', 'DUPLICATE', 'FX_VARIANCE', 'MISSING_COUNTERPARTY')),
    root_cause_category         text,
    -- REOPENED included per plans/docs/05-case-management.md §6.1's lifecycle
    -- diagram, even though §4.1's enum text omits it - see plans/task/core/03
    -- Implementation Notes.
    status                      text NOT NULL CHECK (status IN ('OPEN', 'ASSIGNED', 'IN_PROGRESS', 'PENDING_APPROVAL', 'RESOLVED', 'WRITTEN_OFF', 'ESCALATED', 'REOPENED')),
    assigned_to                 text,
    priority                    text NOT NULL,
    sla_due_at                  timestamptz,
    opened_at                   timestamptz NOT NULL DEFAULT now(),
    resolved_at                 timestamptz,
    amount_at_risk              numeric(20,4),
    currency                    char(3),
    temporal_workflow_id        text,
    created_at                  timestamptz NOT NULL DEFAULT now(),
    updated_at                  timestamptz NOT NULL DEFAULT now()
);

-- Append-only: no updated_at, no ON DELETE CASCADE anywhere near audit tables
-- (plans/task/core/03 Common Pitfalls - cascading deletes must never silently
-- destroy audit history).
CREATE TABLE case_comments (
    id          uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id   uuid NOT NULL REFERENCES tenants(id),
    case_id     uuid NOT NULL REFERENCES cases(id) ON DELETE RESTRICT,
    actor       text NOT NULL,
    event_type  text NOT NULL DEFAULT 'comment',
    payload     jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at  timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE case_audit_events (
    id          uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id   uuid NOT NULL REFERENCES tenants(id),
    case_id     uuid NOT NULL REFERENCES cases(id) ON DELETE RESTRICT,
    actor       text NOT NULL,
    event_type  text NOT NULL,
    payload     jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at  timestamptz NOT NULL DEFAULT now()
);

-- id is a ULID (26-char, time-sortable, generated application-side per
-- plans/docs/03-canonical-data-model.md §4.1) - stored as text, not uuid.
-- hash_chain_prev linking logic is plans/task/core/14, not this task - this
-- migration only creates the column.
CREATE TABLE audit_events (
    id                  text PRIMARY KEY,
    tenant_id           uuid NOT NULL REFERENCES tenants(id),
    actor_id            text,
    actor_type          text NOT NULL,
    event_type          text NOT NULL,
    entity_type         text NOT NULL,
    entity_id           text NOT NULL,
    before_state        jsonb,
    after_state         jsonb,
    ip_address          inet,
    request_id          text,
    occurred_at         timestamptz NOT NULL DEFAULT now(),
    hash_chain_prev     text
);

-- Authoritative dedup table - plans/task/core/09 upserts into this, does not
-- create it (plans/task/core/03 Common Pitfalls).
CREATE TABLE ingestion_dedup (
    id                      uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id               uuid NOT NULL REFERENCES tenants(id),
    idempotency_key         text NOT NULL,
    source_connector_id     uuid REFERENCES connectors(id),
    ingestion_batch_id      text,
    created_at              timestamptz NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, idempotency_key)
);

-- === Indexes (plans/docs/04-matching-engine.md §5.5) =======================

CREATE INDEX idx_transactions_tenant_account_date_amount
    ON transactions (tenant_id, account_id, value_date, base_amount);
CREATE INDEX idx_transactions_tenant_unmatched
    ON transactions (tenant_id, account_id)
    WHERE status = 'UNMATCHED';
CREATE INDEX idx_transactions_created_at_brin ON transactions USING brin (created_at);
CREATE INDEX idx_transactions_value_date_brin ON transactions USING brin (value_date);

CREATE INDEX idx_cases_tenant_status ON cases (tenant_id, status);
CREATE INDEX idx_match_results_tenant_status ON match_results (tenant_id, status);
CREATE INDEX idx_statements_tenant_account_status ON statements (tenant_id, account_id, status);

CREATE INDEX idx_accounts_tenant_id ON accounts (tenant_id);
CREATE INDEX idx_connectors_tenant_id ON connectors (tenant_id);
CREATE INDEX idx_match_rules_tenant_id ON match_rules (tenant_id);
CREATE INDEX idx_match_result_lines_tenant_id ON match_result_lines (tenant_id);
CREATE INDEX idx_case_comments_tenant_case ON case_comments (tenant_id, case_id);
CREATE INDEX idx_case_audit_events_tenant_case ON case_audit_events (tenant_id, case_id);
CREATE INDEX idx_audit_events_tenant_id ON audit_events (tenant_id);
CREATE INDEX idx_ingestion_dedup_tenant_id ON ingestion_dedup (tenant_id);

-- === Row-Level Security ======================================================
-- Every tenant-scoped table below. Tenant Registry tables (tenants,
-- tenant_settings, tenant_isolation_config, tenant_quota, tenant_feature_flags)
-- are unsharded/not tenant-owned - no RLS on those, per plans/task/core/03 §Scope.
--
-- FORCE ROW LEVEL SECURITY is required on every one of these - without it the
-- table owner role bypasses RLS silently (plans/task/core/03 Common Pitfalls).

ALTER TABLE accounts ENABLE ROW LEVEL SECURITY;
ALTER TABLE accounts FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON accounts
    USING (tenant_id = current_setting('app.current_tenant_id')::uuid);

ALTER TABLE connectors ENABLE ROW LEVEL SECURITY;
ALTER TABLE connectors FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON connectors
    USING (tenant_id = current_setting('app.current_tenant_id')::uuid);

ALTER TABLE statements ENABLE ROW LEVEL SECURITY;
ALTER TABLE statements FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON statements
    USING (tenant_id = current_setting('app.current_tenant_id')::uuid);

ALTER TABLE transactions ENABLE ROW LEVEL SECURITY;
ALTER TABLE transactions FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON transactions
    USING (tenant_id = current_setting('app.current_tenant_id')::uuid);

ALTER TABLE match_rules ENABLE ROW LEVEL SECURITY;
ALTER TABLE match_rules FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON match_rules
    USING (tenant_id = current_setting('app.current_tenant_id')::uuid);

ALTER TABLE match_results ENABLE ROW LEVEL SECURITY;
ALTER TABLE match_results FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON match_results
    USING (tenant_id = current_setting('app.current_tenant_id')::uuid);

ALTER TABLE match_result_lines ENABLE ROW LEVEL SECURITY;
ALTER TABLE match_result_lines FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON match_result_lines
    USING (tenant_id = current_setting('app.current_tenant_id')::uuid);

ALTER TABLE cases ENABLE ROW LEVEL SECURITY;
ALTER TABLE cases FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON cases
    USING (tenant_id = current_setting('app.current_tenant_id')::uuid);

ALTER TABLE case_comments ENABLE ROW LEVEL SECURITY;
ALTER TABLE case_comments FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON case_comments
    USING (tenant_id = current_setting('app.current_tenant_id')::uuid);

ALTER TABLE case_audit_events ENABLE ROW LEVEL SECURITY;
ALTER TABLE case_audit_events FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON case_audit_events
    USING (tenant_id = current_setting('app.current_tenant_id')::uuid);

ALTER TABLE audit_events ENABLE ROW LEVEL SECURITY;
ALTER TABLE audit_events FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON audit_events
    USING (tenant_id = current_setting('app.current_tenant_id')::uuid);

ALTER TABLE ingestion_dedup ENABLE ROW LEVEL SECURITY;
ALTER TABLE ingestion_dedup FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON ingestion_dedup
    USING (tenant_id = current_setting('app.current_tenant_id')::uuid);

-- === Grants for the non-superuser application role =========================
-- jengine_app is neither superuser nor table owner, so RLS actually applies
-- to it (see the CREATE ROLE comment near the top of this file).

GRANT USAGE ON SCHEMA public TO jengine_app;

GRANT SELECT, INSERT, UPDATE, DELETE ON
    tenants, tenant_settings, tenant_isolation_config, tenant_quota, tenant_feature_flags,
    accounts, connectors, statements, transactions,
    match_rules, match_results, match_result_lines,
    cases, case_comments, case_audit_events, audit_events, ingestion_dedup
    TO jengine_app;
