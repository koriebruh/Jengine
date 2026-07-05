package clickhouse

import (
	"context"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/google/uuid"
)

// ClickHouse has no RLS equivalent to Postgres's tenant_isolation
// policies (plans/task/core/03) - every query in this file filters on
// tenant_id explicitly in its WHERE clause as the only isolation
// boundary. There is no defense-in-depth layer here; omitting the
// filter is a cross-tenant data leak, not a missing optimization.

type MatchRateByRuleRow struct {
	RuleID        *uuid.UUID
	Day           time.Time
	Status        string
	MatchCount    uint64
	AvgConfidence float64
}

// MatchRateByRule queries mv_match_rate_by_rule (AggregatingMergeTree -
// state columns merged via -Merge combinators at read time, per that
// table's own design: raw insert-time state, not a final value, is
// what's stored).
func MatchRateByRule(ctx context.Context, conn clickhouse.Conn, tenantID uuid.UUID, from, to time.Time) ([]MatchRateByRuleRow, error) {
	rows, err := conn.Query(ctx, `
		SELECT rule_id, day, status, countMerge(match_count) AS match_count, avgMerge(avg_confidence) AS avg_confidence
		FROM mv_match_rate_by_rule
		WHERE tenant_id = ? AND day >= ? AND day <= ?
		GROUP BY rule_id, day, status
		ORDER BY day, rule_id, status`,
		tenantID, from, to,
	)
	if err != nil {
		return nil, fmt.Errorf("clickhouse: query mv_match_rate_by_rule: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var out []MatchRateByRuleRow
	for rows.Next() {
		var r MatchRateByRuleRow
		if err := rows.Scan(&r.RuleID, &r.Day, &r.Status, &r.MatchCount, &r.AvgConfidence); err != nil {
			return nil, fmt.Errorf("clickhouse: scan mv_match_rate_by_rule row: %w", err)
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

type SLAComplianceRow struct {
	AssignedTo        string
	RootCauseCategory string
	Day               time.Time
	TotalCount        uint64
	BreachedCount     uint64
	MTTRSeconds       float64
}

// SLACompliance queries mv_sla_compliance. Only resolved cases
// contribute (see that MV's own WHERE clause) - an unresolved case has
// no breach/on-time verdict yet.
func SLACompliance(ctx context.Context, conn clickhouse.Conn, tenantID uuid.UUID, from, to time.Time) ([]SLAComplianceRow, error) {
	rows, err := conn.Query(ctx, `
		SELECT assigned_to, root_cause_category, day,
		       countMerge(total_count) AS total_count,
		       countIfMerge(breached_count) AS breached_count,
		       avgMerge(mttr_seconds) AS mttr_seconds
		FROM mv_sla_compliance
		WHERE tenant_id = ? AND day >= ? AND day <= ?
		GROUP BY assigned_to, root_cause_category, day
		ORDER BY day, assigned_to, root_cause_category`,
		tenantID, from, to,
	)
	if err != nil {
		return nil, fmt.Errorf("clickhouse: query mv_sla_compliance: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var out []SLAComplianceRow
	for rows.Next() {
		var r SLAComplianceRow
		if err := rows.Scan(&r.AssignedTo, &r.RootCauseCategory, &r.Day, &r.TotalCount, &r.BreachedCount, &r.MTTRSeconds); err != nil {
			return nil, fmt.Errorf("clickhouse: scan mv_sla_compliance row: %w", err)
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

type BreaksDailyAgingRow struct {
	AccountID   uuid.UUID
	AgingBucket string
	OpenCount   uint64
	ComputedAt  time.Time
}

// BreaksDailyAging queries mv_breaks_daily_aging - a REFRESHABLE MV
// (REFRESH EVERY 1 HOUR, see deploy/clickhouse/ddl.sql's own extensive
// comment on why this one can't be a plain incremental MV). Read
// directly, no -Merge combinators needed: it's a plain MergeTree table
// holding a fully pre-computed snapshot as of ComputedAt, not partial
// aggregate state.
func BreaksDailyAging(ctx context.Context, conn clickhouse.Conn, tenantID uuid.UUID) ([]BreaksDailyAgingRow, error) {
	rows, err := conn.Query(ctx, `
		SELECT account_id, aging_bucket, open_count, computed_at
		FROM mv_breaks_daily_aging
		WHERE tenant_id = ?
		ORDER BY account_id, aging_bucket`,
		tenantID,
	)
	if err != nil {
		return nil, fmt.Errorf("clickhouse: query mv_breaks_daily_aging: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var out []BreaksDailyAgingRow
	for rows.Next() {
		var r BreaksDailyAgingRow
		if err := rows.Scan(&r.AccountID, &r.AgingBucket, &r.OpenCount, &r.ComputedAt); err != nil {
			return nil, fmt.Errorf("clickhouse: scan mv_breaks_daily_aging row: %w", err)
		}
		out = append(out, r)
	}
	return out, rows.Err()
}
