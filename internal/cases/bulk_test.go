package cases_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"

	"github.com/koriebruh/Jengine/internal/cases"
	"github.com/koriebruh/Jengine/internal/tenancy"
	"github.com/koriebruh/Jengine/internal/testutil"
)

// TestBulkTransition_MixedStatusSelectionPartiallyFails is plans/task/core/13's
// DoD integration test for bulk operations: over a mixed-status
// selection, exactly one CaseAuditEvent/AuditEvent is written per batch
// op (not per break), IDs whose transition is invalid are reported in
// BulkResult.Failed, and IDs that succeeded are NOT rolled back because
// one ID in the batch failed.
func TestBulkTransition_MixedStatusSelectionPartiallyFails(t *testing.T) {
	if testing.Short() {
		t.Skip("requires Docker - skipped under -short; run make test-integration")
	}

	db := testutil.StartPostgres(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	tenantID, accountID := seedTenantAndAccount(t, ctx, db)
	svc := newLifecycleService(t, ctx, db)
	tenantCtx := tenancy.WithTenant(ctx, tenancy.TenantContext{TenantID: tenantID})
	actor := cases.Actor{UserID: "analyst-1", Role: "ANALYST"}

	openBreak := func(amount string) uuid.UUID {
		brk, err := svc.OpenBreak(ctx, cases.OpenBreakParams{
			TenantID: tenantID, AccountID: accountID, TransactionIDs: []uuid.UUID{uuid.New()},
			BreakType: "UNMATCHED", AmountAtRisk: decimal.RequireFromString(amount), Currency: "USD",
		})
		if err != nil {
			t.Fatalf("OpenBreak failed: %v", err)
		}
		return brk.ID
	}

	// Two breaks left at OPEN (valid target for BulkAssign... wait, we
	// bulk-TRANSITION to IN_PROGRESS, which requires ASSIGNED first) -
	// set up so 2 are ASSIGNED (valid -> IN_PROGRESS) and 1 stays OPEN
	// (invalid -> IN_PROGRESS directly, must go through ASSIGNED first).
	validA := openBreak("10.00")
	validB := openBreak("20.00")
	invalidC := openBreak("30.00")

	if err := svc.Assign(tenantCtx, validA, "analyst-1", actor); err != nil {
		t.Fatalf("Assign validA failed: %v", err)
	}
	if err := svc.Assign(tenantCtx, validB, "analyst-1", actor); err != nil {
		t.Fatalf("Assign validB failed: %v", err)
	}
	// invalidC stays OPEN - IN_PROGRESS is not reachable directly from OPEN.

	result, err := svc.BulkTransition(tenantCtx, []uuid.UUID{validA, validB, invalidC}, cases.BreakInProgress, actor, "bulk transition test")
	if err != nil {
		t.Fatalf("BulkTransition returned an unexpected top-level error: %v", err)
	}

	if len(result.Succeeded) != 2 {
		t.Errorf("expected 2 succeeded IDs, got %d: %v", len(result.Succeeded), result.Succeeded)
	}
	if len(result.Failed) != 1 {
		t.Errorf("expected 1 failed ID, got %d: %v", len(result.Failed), result.Failed)
	}
	if _, failed := result.Failed[invalidC]; !failed {
		t.Errorf("expected invalidC (%s) to be reported as failed, got Failed=%v", invalidC, result.Failed)
	}

	// Confirm the succeeded IDs actually committed (not rolled back
	// because invalidC failed).
	for _, id := range []uuid.UUID{validA, validB} {
		var status string
		if err := db.Pool.QueryRow(ctx, `SELECT status FROM cases WHERE id = $1`, id).Scan(&status); err != nil {
			t.Fatalf("query status for %s failed: %v", id, err)
		}
		if status != string(cases.BreakInProgress) {
			t.Errorf("expected break %s to have committed to IN_PROGRESS despite invalidC's failure, got %s", id, status)
		}
	}
	// invalidC must remain unchanged (still OPEN).
	var invalidStatus string
	if err := db.Pool.QueryRow(ctx, `SELECT status FROM cases WHERE id = $1`, invalidC).Scan(&invalidStatus); err != nil {
		t.Fatalf("query status for invalidC failed: %v", err)
	}
	if invalidStatus != string(cases.BreakOpen) {
		t.Errorf("expected invalidC to remain OPEN, got %s", invalidStatus)
	}

	// Exactly ONE case_audit_events row for this batch op (not one per
	// succeeded break) - check by event_type, since all 3 breaks are
	// otherwise unrelated case rows.
	var bulkEventCount int
	if err := db.Pool.QueryRow(ctx,
		`SELECT count(*) FROM case_audit_events WHERE event_type = 'break.bulk_transitioned' AND case_id = ANY($1)`,
		[]uuid.UUID{validA, validB},
	).Scan(&bulkEventCount); err != nil {
		t.Fatalf("count bulk case_audit_events failed: %v", err)
	}
	if bulkEventCount != 1 {
		t.Errorf("expected exactly 1 case_audit_events row for the whole batch op, got %d", bulkEventCount)
	}

	var globalBulkEventCount int
	if err := db.Pool.QueryRow(ctx,
		`SELECT count(*) FROM audit_events WHERE tenant_id = $1 AND event_type = 'break.bulk_transitioned'`,
		tenantID,
	).Scan(&globalBulkEventCount); err != nil {
		t.Fatalf("count global bulk audit_events failed: %v", err)
	}
	if globalBulkEventCount != 1 {
		t.Errorf("expected exactly 1 global AuditEvent for the whole batch op, got %d", globalBulkEventCount)
	}
}
