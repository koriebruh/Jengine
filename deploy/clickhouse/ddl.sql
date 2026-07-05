-- plans/task/core/22: ClickHouse analytical store DDL. Applied via
-- scripts/apply-clickhouse-ddl.sh against the local dev ClickHouse
-- (deploy/clickhouse's docker-compose service). Debezium's raw CDC
-- envelope (see deploy/debezium/cdc-*-connector.json) arrives via three
-- Kafka Engine tables, gets upserted into three ReplacingMergeTree
-- detail tables, and feeds three materialized views the frontend
-- Overview Dashboard consumes.
--
-- Debezium op codes handled explicitly per table (plans/task/core/22
-- Implementation Notes): 'r' (snapshot read) and 'c'/'u' (create/
-- update) all upsert via ReplacingMergeTree(version); 'd' (delete) sets
-- is_deleted=1 via ReplacingMergeTree's tombstone-column form rather
-- than being filtered out - a plain MergeTree would accumulate every
-- version as a separate row, which is the Common Pitfall this task's
-- own text calls out by name.

CREATE DATABASE IF NOT EXISTS jengine;
USE jengine;

-- =========================================================================
-- transactions
-- =========================================================================

-- `before` is populated (real values, not the CDC pipeline getting no
-- data) on delete events (op='d') by migrations/0012's REPLICA IDENTITY
-- FULL on this table - without it, Postgres's default replica identity
-- only guarantees the primary key survives into a DELETE's `before`,
-- and every other column comes through as a zero-value default rather
-- than the row's real last-known state.
CREATE TABLE IF NOT EXISTS cdc_transactions_kafka
(
    op     String,
    before String,
    after  String,
    ts_ms  UInt64
) ENGINE = Kafka
SETTINGS kafka_broker_list = 'redpanda:29092',
         kafka_topic_list = 'postgres.public.transactions',
         kafka_group_name = 'clickhouse_cdc_transactions',
         kafka_format = 'JSONEachRow';

CREATE TABLE IF NOT EXISTS transactions_local
(
    id                UUID,
    tenant_id         UUID,
    account_id        UUID,
    external_ref      String,
    amount            Decimal(20, 4),
    currency          LowCardinality(String),
    base_amount       Decimal(20, 4),
    value_date        Date,
    booking_date      Date,
    counterparty_ref  String,
    side              LowCardinality(String),
    source_mode       LowCardinality(String),
    status            LowCardinality(String),
    created_at        DateTime64(6),
    updated_at        DateTime64(6),
    version           UInt64,
    is_deleted        UInt8
)
ENGINE = ReplacingMergeTree(version, is_deleted)
ORDER BY (tenant_id, id)
-- Warm-tier TTL (plans/docs/08-storage-architecture.md §9.4: "2-3 years
-- aggregated + detail") - configured now, not deferred, per this task's
-- own Non-Goals ("Do not skip the TTL/tiered-storage configuration").
TTL toDateTime(created_at) + INTERVAL 3 YEAR DELETE;

CREATE MATERIALIZED VIEW IF NOT EXISTS cdc_transactions_mv TO transactions_local AS
WITH if(op = 'd', before, after) AS src
SELECT
    toUUID(JSONExtractString(src, 'id'))                AS id,
    toUUID(JSONExtractString(src, 'tenant_id'))          AS tenant_id,
    toUUID(JSONExtractString(src, 'account_id'))         AS account_id,
    JSONExtractString(src, 'external_ref')               AS external_ref,
    toDecimal64(JSONExtractString(src, 'amount'), 4)     AS amount,
    JSONExtractString(src, 'currency')                   AS currency,
    toDecimal64(JSONExtractString(src, 'base_amount'), 4) AS base_amount,
    toDate(JSONExtractUInt(src, 'value_date'))           AS value_date,
    toDate(JSONExtractUInt(src, 'booking_date'))         AS booking_date,
    JSONExtractString(src, 'counterparty_ref')           AS counterparty_ref,
    JSONExtractString(src, 'side')                       AS side,
    JSONExtractString(src, 'source_mode')                AS source_mode,
    JSONExtractString(src, 'status')                     AS status,
    parseDateTime64BestEffort(JSONExtractString(src, 'created_at')) AS created_at,
    parseDateTime64BestEffort(JSONExtractString(src, 'updated_at')) AS updated_at,
    ts_ms                                                    AS version,
    if(op = 'd', 1, 0)                                       AS is_deleted
FROM cdc_transactions_kafka;

-- =========================================================================
-- match_results
-- =========================================================================

CREATE TABLE IF NOT EXISTS cdc_match_results_kafka
(
    op     String,
    before String,
    after  String,
    ts_ms  UInt64
) ENGINE = Kafka
SETTINGS kafka_broker_list = 'redpanda:29092',
         kafka_topic_list = 'postgres.public.match_results',
         kafka_group_name = 'clickhouse_cdc_match_results',
         kafka_format = 'JSONEachRow';

CREATE TABLE IF NOT EXISTS match_results_local
(
    id                UUID,
    tenant_id         UUID,
    rule_id           Nullable(UUID),
    match_type        LowCardinality(String),
    confidence_score  Decimal(4, 3),
    status            LowCardinality(String),
    matched_at        DateTime64(6),
    created_at        DateTime64(6),
    version           UInt64,
    is_deleted        UInt8
)
ENGINE = ReplacingMergeTree(version, is_deleted)
ORDER BY (tenant_id, id)
TTL toDateTime(created_at) + INTERVAL 3 YEAR DELETE;

CREATE MATERIALIZED VIEW IF NOT EXISTS cdc_match_results_mv TO match_results_local AS
WITH if(op = 'd', before, after) AS src
SELECT
    toUUID(JSONExtractString(src, 'id'))                        AS id,
    toUUID(JSONExtractString(src, 'tenant_id'))                  AS tenant_id,
    -- rule_id is nullable in Postgres (unmatched-by-rule results
    -- shouldn't be possible in practice, but the column itself allows
    -- NULL) - empty-string extraction from a JSON null maps to NULL
    -- here rather than toUUID('') erroring.
    if(JSONExtractString(src, 'rule_id') = '', NULL, toUUID(JSONExtractString(src, 'rule_id'))) AS rule_id,
    JSONExtractString(src, 'match_type')                          AS match_type,
    toDecimal64(JSONExtractString(src, 'confidence_score'), 3)   AS confidence_score,
    JSONExtractString(src, 'status')                              AS status,
    parseDateTime64BestEffort(JSONExtractString(src, 'matched_at')) AS matched_at,
    parseDateTime64BestEffort(JSONExtractString(src, 'created_at')) AS created_at,
    ts_ms                                                            AS version,
    if(op = 'd', 1, 0)                                              AS is_deleted
FROM cdc_match_results_kafka;

-- mv_match_rate_by_rule: naturally incremental, insert-triggered
-- (plans/task/core/22 Implementation Notes: "a MatchResult row's
-- classification doesn't change relative to 'now'" - unlike aging,
-- there's no reason to ever recompute this from scratch on a schedule).
CREATE TABLE IF NOT EXISTS mv_match_rate_by_rule
(
    tenant_id       UUID,
    rule_id         Nullable(UUID),
    day             Date,
    status          LowCardinality(String),
    match_count     AggregateFunction(count),
    avg_confidence  AggregateFunction(avg, Decimal(4, 3))
)
ENGINE = AggregatingMergeTree
ORDER BY (tenant_id, rule_id, day, status)
SETTINGS allow_nullable_key = 1;

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_match_rate_by_rule_mv TO mv_match_rate_by_rule AS
SELECT
    tenant_id,
    rule_id,
    toDate(matched_at)        AS day,
    status,
    countState()              AS match_count,
    avgState(confidence_score) AS avg_confidence
FROM match_results_local
WHERE is_deleted = 0
GROUP BY tenant_id, rule_id, day, status;

-- =========================================================================
-- cases (breaks)
-- =========================================================================

CREATE TABLE IF NOT EXISTS cdc_cases_kafka
(
    op     String,
    before String,
    after  String,
    ts_ms  UInt64
) ENGINE = Kafka
SETTINGS kafka_broker_list = 'redpanda:29092',
         kafka_topic_list = 'postgres.public.cases',
         kafka_group_name = 'clickhouse_cdc_cases_v2',
         kafka_format = 'JSONEachRow';

CREATE TABLE IF NOT EXISTS cases_local
(
    id                    UUID,
    tenant_id             UUID,
    account_id            UUID,
    break_type            LowCardinality(String),
    root_cause_category   String,
    status                LowCardinality(String),
    -- No "team" concept exists in this schema yet (plans/task/core/03's
    -- cases table has assigned_to, an individual, not a team) - used as
    -- the closest available grouping dimension for mv_sla_compliance,
    -- documented here rather than silently invented as a real "team"
    -- column.
    assigned_to           String,
    priority              LowCardinality(String),
    sla_due_at            Nullable(DateTime64(6)),
    opened_at             DateTime64(6),
    resolved_at           Nullable(DateTime64(6)),
    created_at            DateTime64(6),
    version               UInt64,
    is_deleted            UInt8
)
ENGINE = ReplacingMergeTree(version, is_deleted)
ORDER BY (tenant_id, id)
TTL toDateTime(created_at) + INTERVAL 3 YEAR DELETE;

CREATE MATERIALIZED VIEW IF NOT EXISTS cdc_cases_mv TO cases_local AS
WITH if(op = 'd', before, after) AS src
SELECT
    toUUID(JSONExtractString(src, 'id'))                    AS id,
    toUUID(JSONExtractString(src, 'tenant_id'))              AS tenant_id,
    toUUID(JSONExtractString(src, 'account_id'))             AS account_id,
    JSONExtractString(src, 'break_type')                     AS break_type,
    JSONExtractString(src, 'root_cause_category')            AS root_cause_category,
    JSONExtractString(src, 'status')                         AS status,
    JSONExtractString(src, 'assigned_to')                    AS assigned_to,
    JSONExtractString(src, 'priority')                       AS priority,
    nullIf(parseDateTime64BestEffortOrZero(JSONExtractString(src, 'sla_due_at')), toDateTime64(0, 6)) AS sla_due_at,
    parseDateTime64BestEffort(JSONExtractString(src, 'opened_at')) AS opened_at,
    nullIf(parseDateTime64BestEffortOrZero(JSONExtractString(src, 'resolved_at')), toDateTime64(0, 6)) AS resolved_at,
    parseDateTime64BestEffort(JSONExtractString(src, 'created_at')) AS created_at,
    ts_ms                                                       AS version,
    if(op = 'd', 1, 0)                                          AS is_deleted
FROM cdc_cases_kafka;

-- mv_sla_compliance: naturally incremental (plans/task/core/22
-- Implementation Notes: "breach/on-time determination... is computed
-- once, at case-resolution time, from data already in the CDC event -
-- it doesn't depend on current wall-clock"). Only resolved cases
-- contribute - an open case has no verdict yet by definition.
CREATE TABLE IF NOT EXISTS mv_sla_compliance
(
    tenant_id           UUID,
    assigned_to         String,
    root_cause_category String,
    day                 Date,
    total_count         AggregateFunction(count),
    breached_count      AggregateFunction(countIf, UInt8),
    mttr_seconds        AggregateFunction(avg, Int64)
)
ENGINE = AggregatingMergeTree
ORDER BY (tenant_id, assigned_to, root_cause_category, day);

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_sla_compliance_mv TO mv_sla_compliance AS
SELECT
    tenant_id,
    assigned_to,
    root_cause_category,
    toDate(assumeNotNull(resolved_at))                           AS day,
    countState()                                                  AS total_count,
    countIfState(if(sla_due_at IS NOT NULL AND resolved_at > sla_due_at, 1, 0)) AS breached_count,
    avgState(dateDiff('second', opened_at, assumeNotNull(resolved_at))) AS mttr_seconds
FROM cases_local
WHERE is_deleted = 0 AND resolved_at IS NOT NULL
GROUP BY tenant_id, assigned_to, root_cause_category, day;

-- mv_breaks_daily_aging: DELIBERATELY NOT a plain insert-triggered MV
-- (plans/task/core/22 Implementation Notes' own explicit warning: aging
-- bucket is relative to "now", which changes independent of any insert
-- event - an open case's bucket must advance even with zero new
-- events). A refreshable MV recomputes the full aggregation from
-- cases_local on a schedule instead. Do NOT "simplify" this into an
-- incremental MV - that would silently freeze every case's aging
-- bucket at whatever it was when the case was last touched, which is
-- exactly the bug this task's Common Pitfalls names first.
CREATE TABLE IF NOT EXISTS mv_breaks_daily_aging
(
    tenant_id     UUID,
    account_id    UUID,
    aging_bucket  LowCardinality(String),
    open_count    UInt64,
    computed_at   DateTime
)
ENGINE = MergeTree
ORDER BY (tenant_id, account_id, aging_bucket);

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_breaks_daily_aging_mv
REFRESH EVERY 1 HOUR TO mv_breaks_daily_aging AS
SELECT
    tenant_id,
    account_id,
    multiIf(
        dateDiff('day', opened_at, now()) < 1, '0-1d',
        dateDiff('day', opened_at, now()) < 3, '1-3d',
        dateDiff('day', opened_at, now()) < 7, '3-7d',
        dateDiff('day', opened_at, now()) < 30, '7-30d',
        '30+d'
    ) AS aging_bucket,
    count() AS open_count,
    now()   AS computed_at
FROM cases_local
WHERE is_deleted = 0 AND status NOT IN ('RESOLVED', 'WRITTEN_OFF')
GROUP BY tenant_id, account_id, aging_bucket;
