-- plans/task/core/15: Idempotency-Key handling for mutating RPCs.
CREATE TABLE idempotency_requests (
    id              uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id       uuid NOT NULL REFERENCES tenants(id),
    idempotency_key text NOT NULL,
    request_hash    text NOT NULL,
    response_status int NOT NULL,
    response_body   bytea NOT NULL,
    created_at      timestamptz NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, idempotency_key)
);

CREATE INDEX idx_idempotency_requests_created_at ON idempotency_requests (created_at);

ALTER TABLE idempotency_requests ENABLE ROW LEVEL SECURITY;
ALTER TABLE idempotency_requests FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON idempotency_requests
    USING (tenant_id = current_setting('app.current_tenant_id')::uuid);

GRANT SELECT, INSERT, DELETE ON idempotency_requests TO jengine_app;
