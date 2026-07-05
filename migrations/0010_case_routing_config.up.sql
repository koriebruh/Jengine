-- plans/task/core/20: tenant-scoped versioned auto-assignment routing
-- config, consulted by internal/cases/workflow's AutoAssignActivity
-- (plans/docs/05-case-management.md §6.2). Shape mirrors match_rules'
-- own versioning convention (version/status/config jsonb) rather than
-- inventing a different one.
CREATE TABLE case_routing_configs (
    id          uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id   uuid NOT NULL REFERENCES tenants(id),
    version     int NOT NULL,
    status      text NOT NULL CHECK (status IN ('DRAFT', 'ACTIVE', 'ARCHIVED')),
    -- config shape: {"strategy": "round_robin", "team_members": ["user-a", "user-b"]}
    -- strategy is extensible (round_robin implemented at MVP; load_balanced/
    -- skill_based/root_cause_mapping are documented future strategy
    -- values, not yet implemented - see AutoAssignActivity's own doc
    -- comment) without a schema change.
    config      jsonb NOT NULL,
    created_by  text NOT NULL,
    created_at  timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX idx_case_routing_configs_tenant_active ON case_routing_configs (tenant_id) WHERE status = 'ACTIVE';

ALTER TABLE case_routing_configs ENABLE ROW LEVEL SECURITY;
ALTER TABLE case_routing_configs FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON case_routing_configs
    USING (tenant_id = current_setting('app.current_tenant_id')::uuid);

GRANT SELECT, INSERT, UPDATE ON case_routing_configs TO jengine_app;
