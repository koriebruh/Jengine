package cases_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"

	"github.com/koriebruh/Jengine/internal/cases"
	"github.com/koriebruh/Jengine/internal/matching/core"
	"github.com/koriebruh/Jengine/internal/tenancy"
	"github.com/koriebruh/Jengine/internal/testutil"
)

// TestFullLifecycle_OpenToResolved is plans/task/core/13's DoD
// integration test: OpenBreak -> Assign -> Transition(IN_PROGRESS) ->
// AddComment -> RequestApproval -> DecideApproval(approve=true) ->
// confirms Break row state, CaseComment/CaseAuditEvent rows, and that a
// global AuditEvent was appended for each transition.
func TestFullLifecycle_OpenToResolved(t *testing.T) {
	if testing.Short() {
		t.Skip("requires Docker - skipped under -short; run make test-integration")
	}

	db := testutil.StartPostgres(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	tenantID, accountID := seedTenantAndAccount(t, ctx, db)
	svc := newLifecycleService(t, ctx, db)
	tenantCtx := tenancy.WithTenant(ctx, tenancy.TenantContext{TenantID: tenantID})

	requester := cases.Actor{UserID: "analyst-1", Role: "ANALYST"}
	checker := cases.Actor{UserID: "analyst-2", Role: "ANALYST"}

	brk, err := svc.OpenBreak(ctx, cases.OpenBreakParams{
		TenantID: tenantID, AccountID: accountID, TransactionIDs: []uuid.UUID{uuid.New()},
		BreakType: "UNMATCHED", AmountAtRisk: decimal.RequireFromString("250.00"), Currency: "USD",
	})
	if err != nil {
		t.Fatalf("OpenBreak failed: %v", err)
	}
	if brk.Status != cases.BreakOpen {
		t.Fatalf("expected new break status OPEN, got %s", brk.Status)
	}

	if err := svc.Assign(tenantCtx, brk.ID, "analyst-1", requester); err != nil {
		t.Fatalf("Assign failed: %v", err)
	}
	if err := svc.Transition(tenantCtx, brk.ID, cases.BreakInProgress, requester, "investigating"); err != nil {
		t.Fatalf("Transition to IN_PROGRESS failed: %v", err)
	}
	if _, err := svc.AddComment(tenantCtx, brk.ID, requester, "looks like a timing difference"); err != nil {
		t.Fatalf("AddComment failed: %v", err)
	}
	if err := svc.RequestApproval(tenantCtx, brk.ID, requester); err != nil {
		t.Fatalf("RequestApproval failed: %v", err)
	}
	if err := svc.DecideApproval(tenantCtx, brk.ID, checker, true, "confirmed timing difference"); err != nil {
		t.Fatalf("DecideApproval failed: %v", err)
	}

	var finalStatus string
	if err := db.Pool.QueryRow(ctx, `SELECT status FROM cases WHERE id = $1`, brk.ID).Scan(&finalStatus); err != nil {
		t.Fatalf("query final case status failed: %v", err)
	}
	if finalStatus != string(cases.BreakResolved) {
		t.Errorf("expected final status RESOLVED, got %s", finalStatus)
	}

	var commentCount int
	if err := db.Pool.QueryRow(ctx, `SELECT count(*) FROM case_comments WHERE case_id = $1`, brk.ID).Scan(&commentCount); err != nil {
		t.Fatalf("count case_comments failed: %v", err)
	}
	// One from AddComment, one from Transition's own comment param
	// ("investigating"), one from DecideApproval's Transition's comment
	// param ("confirmed timing difference") = 3.
	if commentCount != 3 {
		t.Errorf("expected 3 case_comments rows, got %d", commentCount)
	}

	var caseAuditCount int
	if err := db.Pool.QueryRow(ctx, `SELECT count(*) FROM case_audit_events WHERE case_id = $1`, brk.ID).Scan(&caseAuditCount); err != nil {
		t.Fatalf("count case_audit_events failed: %v", err)
	}
	// break.opened, break.assigned, break.transitioned (IN_PROGRESS),
	// break.commented, approval.requested, break.transitioned (RESOLVED via
	// DecideApproval) = 6.
	if caseAuditCount != 6 {
		t.Errorf("expected 6 case_audit_events rows, got %d", caseAuditCount)
	}

	var globalAuditCount int
	if err := db.Pool.QueryRow(ctx, `SELECT count(*) FROM audit_events WHERE tenant_id = $1 AND entity_id = $2`, tenantID, brk.ID.String()).Scan(&globalAuditCount); err != nil {
		t.Fatalf("count audit_events failed: %v", err)
	}
	// Every case_audit_events write has a matching global AuditEvent -
	// §6.5's two-tier model, both written for every event (plans/task/core/13
	// Common Pitfalls: never write just one).
	if globalAuditCount != caseAuditCount {
		t.Errorf("expected the global audit_events count (%d) to match case_audit_events (%d) - §6.5's two-tier write", globalAuditCount, caseAuditCount)
	}
}

// TestBreakSinkAdapter_OpenBreakProducesCorrectRow is plans/task/core/13's
// DoD test for the core.BreakSink adapter: calling OpenBreak with
// core.OpenBreakParams produces the correct Break row.
func TestBreakSinkAdapter_OpenBreakProducesCorrectRow(t *testing.T) {
	if testing.Short() {
		t.Skip("requires Docker - skipped under -short; run make test-integration")
	}

	db := testutil.StartPostgres(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	tenantID, accountID := seedTenantAndAccount(t, ctx, db)
	svc := newLifecycleService(t, ctx, db)
	adapter := cases.NewBreakSinkAdapter(svc)

	txID := uuid.New()
	err := adapter.OpenBreak(ctx, core.OpenBreakParams{
		TenantID: tenantID, AccountID: accountID, TransactionIDs: []uuid.UUID{txID},
		BreakType: "UNMATCHED", AmountAtRisk: decimal.RequireFromString("42.00"), Currency: "EUR",
	})
	if err != nil {
		t.Fatalf("BreakSinkAdapter.OpenBreak failed: %v", err)
	}

	var status, breakType, currency string
	var amount decimal.Decimal
	var relatedIDs []uuid.UUID
	if err := db.Pool.QueryRow(ctx,
		`SELECT status, break_type, currency, amount_at_risk, related_transaction_ids FROM cases WHERE tenant_id = $1 AND account_id = $2`,
		tenantID, accountID,
	).Scan(&status, &breakType, &currency, &amount, &relatedIDs); err != nil {
		t.Fatalf("query case row failed: %v", err)
	}

	if status != string(cases.BreakOpen) {
		t.Errorf("expected status OPEN, got %s", status)
	}
	if breakType != "UNMATCHED" {
		t.Errorf("expected break_type UNMATCHED, got %s", breakType)
	}
	if currency != "EUR" {
		t.Errorf("expected currency EUR, got %s", currency)
	}
	if !amount.Equal(decimal.RequireFromString("42.00")) {
		t.Errorf("expected amount_at_risk 42.00, got %s", amount)
	}
	if len(relatedIDs) != 1 || relatedIDs[0] != txID {
		t.Errorf("expected related_transaction_ids [%s], got %v", txID, relatedIDs)
	}
}
