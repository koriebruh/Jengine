-- plans/task/core/22: Debezium delete events for tables with the
-- default REPLICA IDENTITY only carry the primary key in `before`
-- (every other column comes through as a zero-value default - empty
-- string, 0.0, epoch timestamp - not the row's real last-known
-- values). ReplacingMergeTree's ORDER BY key (tenant_id, id) needs a
-- REAL tenant_id to correctly co-locate a delete tombstone with the
-- row it's meant to suppress during merges - REPLICA IDENTITY FULL
-- makes Postgres logical replication emit the complete old row on
-- DELETE instead.
ALTER TABLE transactions REPLICA IDENTITY FULL;
ALTER TABLE match_results REPLICA IDENTITY FULL;
ALTER TABLE cases REPLICA IDENTITY FULL;
