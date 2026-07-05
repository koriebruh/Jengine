-- plans/task/core/21: outbound webhook subscriptions + delivery
-- bookkeeping (plans/docs/07-api-extensibility.md §8.2).
CREATE TABLE webhook_subscriptions (
    id           uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id    uuid NOT NULL REFERENCES tenants(id),
    url          text NOT NULL,
    -- secret_ref is a Vault path reference, never an inline secret
    -- value (plans/docs/16-development-workflow.md §16.3) - enforcing
    -- that is the dispatcher's job (internal/notify), not this schema's.
    secret_ref   text NOT NULL,
    event_types  text[] NOT NULL,
    filter_expr  text,
    status       text NOT NULL DEFAULT 'ACTIVE' CHECK (status IN ('ACTIVE', 'PAUSED', 'DISABLED')),
    created_at   timestamptz NOT NULL DEFAULT now(),
    updated_at   timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX idx_webhook_subscriptions_tenant_status ON webhook_subscriptions (tenant_id, status);

ALTER TABLE webhook_subscriptions ENABLE ROW LEVEL SECURITY;
ALTER TABLE webhook_subscriptions FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON webhook_subscriptions
    USING (tenant_id = current_setting('app.current_tenant_id')::uuid);
GRANT SELECT, INSERT, UPDATE ON webhook_subscriptions TO jengine_app;

CREATE TABLE webhook_deliveries (
    id                     uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id              uuid NOT NULL REFERENCES tenants(id),
    subscription_id        uuid NOT NULL REFERENCES webhook_subscriptions(id),
    event_id               text NOT NULL, -- outbox_event.id, as text (bigserial on the source side)
    event_type             text NOT NULL,
    payload                bytea NOT NULL,
    attempt_count          int NOT NULL DEFAULT 0,
    status                 text NOT NULL DEFAULT 'PENDING' CHECK (status IN ('PENDING', 'DELIVERED', 'FAILED', 'DEAD_LETTERED')),
    last_attempt_at        timestamptz,
    next_attempt_at        timestamptz,
    response_status        int,
    -- Truncated (e.g. first 2KB) - never store unbounded response
    -- bodies (plans/task/core/21 Implementation Notes' own data model
    -- comment).
    response_body_snippet  text,
    created_at             timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX idx_webhook_deliveries_tenant_status ON webhook_deliveries (tenant_id, status);
CREATE INDEX idx_webhook_deliveries_subscription ON webhook_deliveries (subscription_id);
-- Dispatcher's retry-scan needs "which PENDING/FAILED deliveries are due
-- for their next attempt" - partial index keeps that scan cheap as the
-- table grows (DELIVERED/DEAD_LETTERED rows are never scanned for retry).
CREATE INDEX idx_webhook_deliveries_next_attempt ON webhook_deliveries (next_attempt_at)
    WHERE status IN ('PENDING', 'FAILED');

ALTER TABLE webhook_deliveries ENABLE ROW LEVEL SECURITY;
ALTER TABLE webhook_deliveries FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON webhook_deliveries
    USING (tenant_id = current_setting('app.current_tenant_id')::uuid);
GRANT SELECT, INSERT, UPDATE ON webhook_deliveries TO jengine_app;
