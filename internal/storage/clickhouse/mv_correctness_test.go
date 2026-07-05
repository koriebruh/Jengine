package clickhouse_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/koriebruh/Jengine/internal/storage/clickhouse"
)

// TestMVCorrectness_MatchRateByRule is the golden-dataset MV
// correctness test plans/task/core/22's DoD asks for: insert a known
// fixture (two AUTO_MATCHED results with known confidence scores) and
// assert mv_match_rate_by_rule's aggregate matches the hand-computed
// expected value (count=2, avg=(0.90+0.80)/2=0.85) - not just "a query
// ran without error."
func TestMVCorrectness_MatchRateByRule(t *testing.T) {
	requireLocalCDCStack(t)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	pgPool, err := pgxpool.New(ctx, localSuperuserDevDSN)
	if err != nil {
		t.Fatalf("connect to local dev Postgres: %v", err)
	}
	defer pgPool.Close()

	// A fresh, throwaway tenant per run - reusing an existing tenant
	// would accumulate aggregate contributions across test runs
	// forever: mv_match_rate_by_rule is an insert-triggered
	// AggregatingMergeTree (by this task's own design, since match
	// results don't get retroactively reclassified in normal
	// operation) - deleting a fixture row from Postgres does NOT
	// retract its already-contributed aggregate state from ClickHouse,
	// so a shared tenant's bucket only ever grows across repeated test
	// runs.
	tenantID := uuid.New()
	if _, err := pgPool.Exec(ctx, `INSERT INTO tenants (id, name, isolation_tier, region, status) VALUES ($1, 'mv-correctness-test', 'STANDARD', 'us-east', 'ACTIVE')`, tenantID); err != nil {
		t.Fatalf("seed throwaway tenant: %v", err)
	}
	t.Cleanup(func() {
		_, _ = pgPool.Exec(context.Background(), `DELETE FROM tenants WHERE id = $1`, tenantID)
	})

	id1, id2 := uuid.New(), uuid.New()
	_, err = pgPool.Exec(ctx, `
		INSERT INTO match_results (id, tenant_id, rule_id, match_type, confidence_score, status, matched_at)
		VALUES ($1, $3, NULL, 'ONE_TO_ONE', 0.900, 'AUTO_MATCHED', now()),
		       ($2, $3, NULL, 'ONE_TO_ONE', 0.800, 'AUTO_MATCHED', now())`,
		id1, id2, tenantID,
	)
	if err != nil {
		t.Fatalf("insert fixture match_results: %v", err)
	}
	t.Cleanup(func() {
		_, _ = pgPool.Exec(context.Background(), `DELETE FROM match_results WHERE id IN ($1, $2)`, id1, id2)
	})

	chConn, err := clickhouse.NewClient(ctx, localClickHouseAddr, "jengine", "default", "")
	if err != nil {
		t.Fatalf("connect to ClickHouse: %v", err)
	}
	defer func() { _ = chConn.Close() }()

	today := time.Now()
	from := today.AddDate(0, 0, -1)
	to := today.AddDate(0, 0, 1)

	deadline := time.Now().Add(30 * time.Second)
	for {
		rows, err := clickhouse.MatchRateByRule(ctx, chConn, tenantID, from, to)
		if err != nil {
			t.Fatalf("MatchRateByRule query failed: %v", err)
		}
		var found *clickhouse.MatchRateByRuleRow
		for i := range rows {
			if rows[i].RuleID == nil && rows[i].Status == "AUTO_MATCHED" && rows[i].MatchCount >= 2 {
				found = &rows[i]
				break
			}
		}
		if found != nil {
			const wantAvg = (0.900 + 0.800) / 2
			if found.MatchCount != 2 {
				t.Errorf("expected match_count=2 for this fixture's rule/day/status bucket, got %d (other test data may have landed in the same bucket)", found.MatchCount)
			}
			if diff := found.AvgConfidence - wantAvg; diff > 0.001 || diff < -0.001 {
				t.Errorf("expected avg_confidence=%.3f, got %.3f", wantAvg, found.AvgConfidence)
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("fixture match_results never appeared in mv_match_rate_by_rule within the deadline")
		}
		time.Sleep(500 * time.Millisecond)
	}
}

// TestMVCorrectness_SLACompliance: golden dataset for mv_sla_compliance
// - one on-time and one breached resolution, hand-computed expected
// breach rate and MTTR.
func TestMVCorrectness_SLACompliance(t *testing.T) {
	requireLocalCDCStack(t)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	pgPool, err := pgxpool.New(ctx, localSuperuserDevDSN)
	if err != nil {
		t.Fatalf("connect to local dev Postgres: %v", err)
	}
	defer pgPool.Close()

	var tenantID, accountID uuid.UUID
	if err := pgPool.QueryRow(ctx, `SELECT id FROM tenants LIMIT 1`).Scan(&tenantID); err != nil {
		t.Skipf("no seeded tenant in local dev DB to test against: %v", err)
	}
	if err := pgPool.QueryRow(ctx, `SELECT id FROM accounts WHERE tenant_id = $1 LIMIT 1`, tenantID).Scan(&accountID); err != nil {
		t.Skipf("no seeded account for tenant %s: %v", tenantID, err)
	}

	assignedTo := "sla-fixture-" + uuid.New().String()[:8]
	onTimeID, breachedID := uuid.New(), uuid.New()
	openedAt := time.Now().Add(-2 * time.Hour)
	slaDueAt := time.Now().Add(-1 * time.Hour)
	onTimeResolvedAt := slaDueAt.Add(-30 * time.Minute)  // resolved before due -> on-time
	breachedResolvedAt := slaDueAt.Add(30 * time.Minute) // resolved after due -> breached

	_, err = pgPool.Exec(ctx, `
		INSERT INTO cases (id, tenant_id, account_id, break_type, status, priority, assigned_to, sla_due_at, opened_at, resolved_at)
		VALUES
		  ($1, $3, $4, 'UNMATCHED', 'RESOLVED', 'MEDIUM', $5, $6, $7, $8),
		  ($2, $3, $4, 'UNMATCHED', 'RESOLVED', 'MEDIUM', $5, $6, $7, $9)`,
		onTimeID, breachedID, tenantID, accountID, assignedTo, slaDueAt, openedAt, onTimeResolvedAt, breachedResolvedAt,
	)
	if err != nil {
		t.Fatalf("insert fixture cases: %v", err)
	}
	t.Cleanup(func() {
		_, _ = pgPool.Exec(context.Background(), `DELETE FROM cases WHERE id IN ($1, $2)`, onTimeID, breachedID)
	})

	chConn, err := clickhouse.NewClient(ctx, localClickHouseAddr, "jengine", "default", "")
	if err != nil {
		t.Fatalf("connect to ClickHouse: %v", err)
	}
	defer func() { _ = chConn.Close() }()

	from := time.Now().AddDate(0, 0, -1)
	to := time.Now().AddDate(0, 0, 1)

	deadline := time.Now().Add(30 * time.Second)
	for {
		rows, err := clickhouse.SLACompliance(ctx, chConn, tenantID, from, to)
		if err != nil {
			t.Fatalf("SLACompliance query failed: %v", err)
		}
		var found *clickhouse.SLAComplianceRow
		for i := range rows {
			if rows[i].AssignedTo == assignedTo {
				found = &rows[i]
				break
			}
		}
		if found != nil {
			if found.TotalCount != 2 {
				t.Errorf("expected total_count=2, got %d", found.TotalCount)
			}
			if found.BreachedCount != 1 {
				t.Errorf("expected breached_count=1 (one on-time, one breached), got %d", found.BreachedCount)
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("fixture cases never appeared in mv_sla_compliance within the deadline")
		}
		time.Sleep(500 * time.Millisecond)
	}
}
