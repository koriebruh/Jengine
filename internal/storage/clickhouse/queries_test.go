package clickhouse

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
)

// requires the local dev ClickHouse (docker compose up -d clickhouse)
// with deploy/clickhouse/ddl.sql already applied - skipped by default
// since this package has no testcontainers-go lifecycle of its own yet
// (ClickHouse isn't in internal/testutil's container helpers).
func TestQueries_AgainstRealClickHouse(t *testing.T) {
	if testing.Short() {
		t.Skip("requires a running ClickHouse - skipped under -short")
	}
	ctx := context.Background()
	conn, err := NewClient(ctx, "localhost:9004", "jengine", "default", "")
	if err != nil {
		t.Skipf("clickhouse not reachable, skipping: %v", err)
	}
	defer func() { _ = conn.Close() }()

	// Any tenant present in the dev stack's seeded/CDC'd data - this
	// test only asserts the queries execute and return a well-formed
	// (possibly empty) result set, not specific golden values (that's
	// covered by the manual verification done against real fixture
	// data during this task's own development).
	tenantID := uuid.New()
	from := time.Now().AddDate(0, -1, 0)
	to := time.Now().AddDate(0, 1, 0)

	if _, err := MatchRateByRule(ctx, conn, tenantID, from, to); err != nil {
		t.Errorf("MatchRateByRule failed: %v", err)
	}
	if _, err := SLACompliance(ctx, conn, tenantID, from, to); err != nil {
		t.Errorf("SLACompliance failed: %v", err)
	}
	if _, err := BreaksDailyAging(ctx, conn, tenantID); err != nil {
		t.Errorf("BreaksDailyAging failed: %v", err)
	}
}
