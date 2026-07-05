package clickhouse_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/koriebruh/Jengine/internal/storage/clickhouse"
)

// TestRefreshableMV_BreaksDailyAging exercises the refreshable-MV
// behavior plans/task/core/22's DoD specifically asks for: insert an
// open case, manually trigger `SYSTEM REFRESH VIEW` (rather than
// waiting for the real 1-hour schedule), and assert the case's aging
// bucket appears - proving mv_breaks_daily_aging recomputes from
// current wall-clock state rather than being frozen at insert time
// (the exact bug this task's Common Pitfalls warns a plain insert-
// triggered MV would have).
func TestRefreshableMV_BreaksDailyAging(t *testing.T) {
	requireLocalCDCStack(t)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	pgPool, err := pgxpool.New(ctx, localSuperuserDevDSN)
	if err != nil {
		t.Fatalf("connect to local dev Postgres: %v", err)
	}
	defer pgPool.Close()

	tenantID := uuid.New()
	if _, err := pgPool.Exec(ctx, `INSERT INTO tenants (id, name, isolation_tier, region, status) VALUES ($1, 'aging-mv-test', 'STANDARD', 'us-east', 'ACTIVE')`, tenantID); err != nil {
		t.Fatalf("seed throwaway tenant: %v", err)
	}
	t.Cleanup(func() {
		_, _ = pgPool.Exec(context.Background(), `DELETE FROM tenants WHERE id = $1`, tenantID)
	})

	var accountID uuid.UUID
	if err := pgPool.QueryRow(ctx, `
		INSERT INTO accounts (tenant_id, external_account_ref, account_type, currency, name)
		VALUES ($1, 'aging-test-acct', 'BANK', 'USD', 'Aging Test Account') RETURNING id`, tenantID,
	).Scan(&accountID); err != nil {
		t.Fatalf("seed account: %v", err)
	}

	caseID := uuid.New()
	openedAt := time.Now().Add(-2 * 24 * time.Hour) // 2 days ago -> expected bucket "1-3d"
	if _, err := pgPool.Exec(ctx, `
		INSERT INTO cases (id, tenant_id, account_id, break_type, status, priority, opened_at)
		VALUES ($1, $2, $3, 'UNMATCHED', 'OPEN', 'MEDIUM', $4)`,
		caseID, tenantID, accountID, openedAt,
	); err != nil {
		t.Fatalf("insert fixture case: %v", err)
	}

	chConn, err := clickhouse.NewClient(ctx, localClickHouseAddr, "jengine", "default", "")
	if err != nil {
		t.Fatalf("connect to ClickHouse: %v", err)
	}
	defer func() { _ = chConn.Close() }()

	// Wait for the case to reach cases_local via CDC before refreshing -
	// the refresh recomputes from whatever's already landed there.
	deadline := time.Now().Add(30 * time.Second)
	for {
		var count uint64
		if err := chConn.QueryRow(ctx, `SELECT count() FROM cases_local FINAL WHERE id = ?`, caseID).Scan(&count); err == nil && count > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("fixture case never reached cases_local within the deadline")
		}
		time.Sleep(500 * time.Millisecond)
	}

	if err := chConn.Exec(ctx, `SYSTEM REFRESH VIEW mv_breaks_daily_aging_mv`); err != nil {
		t.Fatalf("SYSTEM REFRESH VIEW failed: %v", err)
	}
	if err := chConn.Exec(ctx, `SYSTEM WAIT VIEW mv_breaks_daily_aging_mv`); err != nil {
		t.Fatalf("SYSTEM WAIT VIEW failed: %v", err)
	}

	rows, err := clickhouse.BreaksDailyAging(ctx, chConn, tenantID)
	if err != nil {
		t.Fatalf("BreaksDailyAging query failed: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("expected exactly 1 aging bucket row for this fresh tenant, got %d: %+v", len(rows), rows)
	}
	if rows[0].AgingBucket != "1-3d" {
		t.Errorf("expected aging_bucket '1-3d' for a case opened 2 days ago, got %q", rows[0].AgingBucket)
	}
	if rows[0].OpenCount != 1 {
		t.Errorf("expected open_count=1, got %d", rows[0].OpenCount)
	}
}
