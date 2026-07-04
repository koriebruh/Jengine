-- Reverses 0001_init_schema.up.sql. Dropping a table drops its indexes/RLS
-- policies with it, so no separate DROP INDEX/POLICY statements are needed -
-- only table drop order (reverse FK dependency order) matters.

DROP TABLE IF EXISTS ingestion_dedup;
DROP TABLE IF EXISTS audit_events;
DROP TABLE IF EXISTS case_audit_events;
DROP TABLE IF EXISTS case_comments;
DROP TABLE IF EXISTS cases;
DROP TABLE IF EXISTS match_result_lines;
DROP TABLE IF EXISTS match_results;
DROP TABLE IF EXISTS match_rules;
DROP TABLE IF EXISTS transactions;
DROP TABLE IF EXISTS statements;
DROP TABLE IF EXISTS connectors;
DROP TABLE IF EXISTS accounts;
DROP TABLE IF EXISTS tenant_feature_flags;
DROP TABLE IF EXISTS tenant_quota;
DROP TABLE IF EXISTS tenant_isolation_config;
DROP TABLE IF EXISTS tenant_settings;
DROP TABLE IF EXISTS tenants;

DROP EXTENSION IF EXISTS pgcrypto;

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'jengine_app') THEN
        REVOKE ALL ON SCHEMA public FROM jengine_app;
        DROP ROLE jengine_app;
    END IF;
END
$$;
