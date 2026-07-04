-- Gap found during plans/task/core/06: plans/docs/02-data-ingestion.md
-- §3.3 requires a durable quarantine queue for failed records ("never
-- silently drop financial data"), but this table isn't in task 03's
-- §4.1 entity list. Added here per task 03's own expand-contract
-- convention (new migration, not editing the already-applied 0001/0002).

CREATE TABLE quarantine_entries (
    id           uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id    uuid NOT NULL REFERENCES tenants(id),
    connector_id uuid REFERENCES connectors(id),
    stage        text NOT NULL,
    reason       text NOT NULL,
    raw_payload  bytea NOT NULL,
    occurred_at  timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX idx_quarantine_entries_tenant_connector ON quarantine_entries (tenant_id, connector_id);

ALTER TABLE quarantine_entries ENABLE ROW LEVEL SECURITY;
ALTER TABLE quarantine_entries FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON quarantine_entries
    USING (tenant_id = current_setting('app.current_tenant_id')::uuid);

GRANT SELECT, INSERT, UPDATE, DELETE ON quarantine_entries TO jengine_app;
