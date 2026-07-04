-- Gap found during plans/task/core/06: the transactional-outbox pattern
-- (plans/docs/06-streaming-architecture.md §7.3) needs a durable outbox
-- table, not in task 03's original §4.1 entity list. Added per task 03's
-- expand-contract convention (new migration).
--
-- RLS note: this table HAS RLS (writes always happen within a single
-- tenant's transaction via PersistEmitStage). The relay/poller that later
-- sweeps ALL tenants' unsent rows and publishes them is a deliberate,
-- narrow exception - it runs against the migration/superuser connection,
-- not a tenant-scoped one, for the same reason migrations do: it is
-- genuinely cross-tenant infrastructure, not per-tenant application
-- logic, and RLS's single current_tenant_id session variable cannot
-- represent "all tenants" at once. See internal/storage/postgres/outbox_repo.go.

CREATE TABLE ingestion_outbox (
    id         uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id  uuid NOT NULL REFERENCES tenants(id),
    topic      text NOT NULL,
    key        text NOT NULL,
    payload    bytea NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    sent_at    timestamptz
);

CREATE INDEX idx_ingestion_outbox_unsent ON ingestion_outbox (created_at) WHERE sent_at IS NULL;
CREATE INDEX idx_ingestion_outbox_tenant_id ON ingestion_outbox (tenant_id);

ALTER TABLE ingestion_outbox ENABLE ROW LEVEL SECURITY;
ALTER TABLE ingestion_outbox FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON ingestion_outbox
    USING (tenant_id = current_setting('app.current_tenant_id')::uuid);

GRANT SELECT, INSERT, UPDATE, DELETE ON ingestion_outbox TO jengine_app;
