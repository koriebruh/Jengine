-- plans/task/core/14: enforce append-only-ness for audit_events at the
-- Postgres role level, not application convention - the actual guarantee
-- the hash-chain design relies on (plans/docs/09-security-compliance.md
-- §10.1). Task 03/0001 granted jengine_app SELECT/INSERT/UPDATE/DELETE on
-- audit_events like every other table; narrow it here now that this
-- table's append-only requirement is concrete.
REVOKE UPDATE, DELETE ON audit_events FROM jengine_app;

-- Per-tenant chain-tail tracking, one row per tenant, updated
-- transactionally alongside every audit_events insert. Concurrency
-- control mechanism (plans/task/core/14 Implementation Notes): a writer
-- takes `SELECT ... FOR UPDATE` on this tenant's row before computing
-- the next event's hash, serializing chain-extension per tenant without
-- serializing writes ACROSS tenants (a global lock/table would create
-- unnecessary cross-tenant contention - plans/task/core/14 Common
-- Pitfalls explicitly calls out chaining globally as the wrong choice).
CREATE TABLE audit_chain_tail (
    tenant_id       uuid PRIMARY KEY REFERENCES tenants(id),
    last_event_id   text NOT NULL,
    last_hash       text NOT NULL,
    updated_at      timestamptz NOT NULL DEFAULT now()
);

-- RLS applies normally here (unlike ingestion_outbox's documented
-- cross-tenant exception): every audit.Writer.Write call runs inside the
-- same tenant-scoped transaction as the operation it's auditing (task
-- 13's Transition, etc.), which has already set app.current_tenant_id -
-- there's no single sweep-all-tenants access pattern for this table.
ALTER TABLE audit_chain_tail ENABLE ROW LEVEL SECURITY;
ALTER TABLE audit_chain_tail FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON audit_chain_tail
    USING (tenant_id = current_setting('app.current_tenant_id')::uuid);

GRANT SELECT, INSERT, UPDATE ON audit_chain_tail TO jengine_app;
