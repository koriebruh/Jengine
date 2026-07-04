-- Gap found during plans/task/core/08: neither table is in task 03's
-- original §4.1 entity list. Added per task 03's expand-contract
-- convention (new migration, not editing already-applied ones).

-- mapping_specs: versioned, tenant-configurable field-mapping DSL specs
-- (plans/docs/02-data-ingestion.md §3.2). Mirrors match_rules' DRAFT/
-- ACTIVE/ARCHIVED status convention for consistency (plans/task/core/08
-- Implementation Notes), though mapping specs don't need match_rules'
-- maker-checker approval gate - lower blast radius, still versioned for
-- audit/rollback.
CREATE TABLE mapping_specs (
    id            uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id     uuid NOT NULL REFERENCES tenants(id),
    source_format text NOT NULL,
    version       int NOT NULL,
    status        text NOT NULL DEFAULT 'ACTIVE' CHECK (status IN ('DRAFT', 'ACTIVE', 'ARCHIVED')),
    spec          jsonb NOT NULL,
    created_at    timestamptz NOT NULL DEFAULT now(),
    updated_at    timestamptz NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, source_format, version)
);

CREATE INDEX idx_mapping_specs_tenant_format_status ON mapping_specs (tenant_id, source_format, status);

ALTER TABLE mapping_specs ENABLE ROW LEVEL SECURITY;
ALTER TABLE mapping_specs FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON mapping_specs
    USING (tenant_id = current_setting('app.current_tenant_id')::uuid);

GRANT SELECT, INSERT, UPDATE, DELETE ON mapping_specs TO jengine_app;

-- fx_rates: static rate-table lookup for base-currency normalization
-- (plans/docs/03-canonical-data-model.md §4.2's simpler alternative to a
-- live FX-provider connector, explicitly allowed for MVP). One current
-- rate per (tenant, from, to) - not date-ranged; periodic updates
-- overwrite the row rather than appending history, sufficient for MVP.
CREATE TABLE fx_rates (
    id            uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id     uuid NOT NULL REFERENCES tenants(id),
    from_currency text NOT NULL,
    to_currency   text NOT NULL,
    rate          numeric(20,10) NOT NULL,
    effective_date date NOT NULL,
    created_at    timestamptz NOT NULL DEFAULT now(),
    updated_at    timestamptz NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, from_currency, to_currency)
);

CREATE INDEX idx_fx_rates_tenant_pair ON fx_rates (tenant_id, from_currency, to_currency);

ALTER TABLE fx_rates ENABLE ROW LEVEL SECURITY;
ALTER TABLE fx_rates FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON fx_rates
    USING (tenant_id = current_setting('app.current_tenant_id')::uuid);

GRANT SELECT, INSERT, UPDATE, DELETE ON fx_rates TO jengine_app;
