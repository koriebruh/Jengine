-- plans/task/core/18: general transactional-outbox table, consumed via
-- Debezium's outbox-event-router SMT (CDC-driven), not a Go poller -
-- see internal/platform/outbox's package doc for how this differs from
-- (and doesn't replace) task 06/09's ingestion_outbox+OutboxRelay, which
-- stays as the ingestion pipeline's own simpler poll-based mechanism.
-- QA_REPORT.md documents this as an open architectural question (two
-- outbox mechanisms with overlapping intent) for a human to reconcile.
CREATE TABLE outbox_event (
    id             bigserial PRIMARY KEY,
    tenant_id      uuid NOT NULL REFERENCES tenants(id),
    aggregate_type text NOT NULL, -- 'transaction' | 'match_result' | 'break' | 'webhook' | ...
    aggregate_id   uuid NOT NULL,
    event_type     text NOT NULL,
    topic          text NOT NULL,
    payload        bytea NOT NULL, -- serialized protobuf message
    created_at     timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX idx_outbox_event_created_at ON outbox_event (created_at);

ALTER TABLE outbox_event ENABLE ROW LEVEL SECURITY;
ALTER TABLE outbox_event FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON outbox_event
    USING (tenant_id = current_setting('app.current_tenant_id')::uuid);

-- INSERT/SELECT only - Debezium's CDC connector reads via logical
-- replication (as a different role), the application role never needs
-- UPDATE/DELETE since rows are immutable once written (mirrors
-- audit_events' append-only enforcement, plans/task/core/14).
GRANT SELECT, INSERT ON outbox_event TO jengine_app;
-- bigserial's implicit sequence needs its own grant - INSERT alone
-- isn't enough, since inserting a bigserial column calls nextval()
-- under the hood, which requires USAGE on the sequence. Found via a
-- real "permission denied for sequence outbox_event_id_seq" error
-- (no other table in this schema uses bigserial/serial - they're all
-- uuid PRIMARY KEY DEFAULT gen_random_uuid(), which needs no sequence
-- grant - so this class of bug never surfaced before this table).
GRANT USAGE, SELECT ON SEQUENCE outbox_event_id_seq TO jengine_app;
