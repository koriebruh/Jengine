-- Gap found during plans/task/core/04: the RegistryRepo interface requires
-- GetTenantByAPIKeyHash, but plans/task/core/03's tenants table has no
-- column/table backing API-key lookup. Small, unambiguous addition per
-- plans/task/core/03's own expand-contract convention (new migration, not
-- editing the already-applied 0001).
--
-- Registry-scoped like the Tenant Registry tables (tenants,
-- tenant_settings, ...) - no RLS here, same reasoning: looking up "which
-- tenant does this API key belong to" necessarily happens before
-- app.current_tenant_id is known, so an RLS policy keyed on that session
-- variable would be a chicken-and-egg problem for this exact table.

CREATE TABLE tenant_api_keys (
    id          uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id   uuid NOT NULL REFERENCES tenants(id),
    key_hash    text NOT NULL UNIQUE,
    created_at  timestamptz NOT NULL DEFAULT now(),
    revoked_at  timestamptz
);

CREATE INDEX idx_tenant_api_keys_tenant_id ON tenant_api_keys (tenant_id);

GRANT SELECT, INSERT, UPDATE, DELETE ON tenant_api_keys TO jengine_app;
