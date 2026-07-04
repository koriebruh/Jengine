package apiserver_test

import (
	"context"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/google/uuid"
	"github.com/shopspring/decimal"

	jenginev1 "github.com/koriebruh/Jengine/gen/go/jengine/v1"
	"github.com/koriebruh/Jengine/internal/apiserver"
	"github.com/koriebruh/Jengine/internal/audit"
	"github.com/koriebruh/Jengine/internal/cases"
	"github.com/koriebruh/Jengine/internal/storage/postgres"
	"github.com/koriebruh/Jengine/internal/tenancy"
	"github.com/koriebruh/Jengine/internal/testutil"
)

// TestBreakService_AssignAddCommentGet is plans/task/core/15's DoD
// integration test: AssignBreak -> AddComment -> visible via GetBreak.
func TestBreakService_AssignAddCommentGet(t *testing.T) {
	if testing.Short() {
		t.Skip("requires Docker - skipped under -short; run make test-integration")
	}

	db := testutil.StartPostgres(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	tenantID, accountA, _ := seedTenantWithTwoAccounts(t, ctx, db)
	appPool := testutil.AppRolePool(t, ctx, db.DSN)
	defer appPool.Close()
	tenantCtx := tenancy.WithTenant(ctx, tenancy.TenantContext{TenantID: tenantID})

	txRunner := func(ctx context.Context, tid uuid.UUID, fn func(ctx context.Context) error) error {
		return postgres.WithTx(tenancy.WithTenant(ctx, tenancy.TenantContext{TenantID: tid}), appPool, tid, fn)
	}
	lifecycle := cases.NewPostgresLifecycleService(txRunner, postgres.NewCaseRepo(), audit.NewPostgresWriter())

	brk, err := lifecycle.OpenBreak(ctx, cases.OpenBreakParams{
		TenantID: tenantID, AccountID: accountA, TransactionIDs: []uuid.UUID{uuid.New()},
		BreakType: "UNMATCHED", AmountAtRisk: decimal.RequireFromString("50.00"), Currency: "USD",
	})
	if err != nil {
		t.Fatalf("OpenBreak failed: %v", err)
	}

	h := &apiserver.BreakServiceHandler{
		Pool: appPool, Cases: postgres.NewCaseRepo(), Lifecycle: lifecycle,
		Idempotency: apiserver.NewPostgresIdempotencyStore(appPool),
	}

	assignReq := connect.NewRequest(&jenginev1.AssignBreakRequest{
		Id: brk.ID.String(), Assignee: "analyst-1", Actor: &jenginev1.Actor{UserId: "analyst-1", Role: "ANALYST"},
	})
	assignReq.Header().Set("Idempotency-Key", uuid.New().String())
	if _, err := h.AssignBreak(tenantCtx, assignReq); err != nil {
		t.Fatalf("AssignBreak failed: %v", err)
	}

	commentReq := connect.NewRequest(&jenginev1.AddCommentRequest{
		Id: brk.ID.String(), Actor: &jenginev1.Actor{UserId: "analyst-1", Role: "ANALYST"}, Body: "investigating this break",
	})
	commentReq.Header().Set("Idempotency-Key", uuid.New().String())
	if _, err := h.AddComment(tenantCtx, commentReq); err != nil {
		t.Fatalf("AddComment failed: %v", err)
	}

	getResp, err := h.GetBreak(tenantCtx, connect.NewRequest(&jenginev1.GetBreakRequest{Id: brk.ID.String()}))
	if err != nil {
		t.Fatalf("GetBreak failed: %v", err)
	}
	if getResp.Msg.Brk.Status != "ASSIGNED" {
		t.Errorf("expected status ASSIGNED, got %s", getResp.Msg.Brk.Status)
	}
	if getResp.Msg.Brk.AssignedTo != "analyst-1" {
		t.Errorf("expected assigned_to analyst-1, got %s", getResp.Msg.Brk.AssignedTo)
	}

	var commentCount int
	if err := db.Pool.QueryRow(ctx, `SELECT count(*) FROM case_comments WHERE case_id = $1`, brk.ID).Scan(&commentCount); err != nil {
		t.Fatalf("count comments failed: %v", err)
	}
	if commentCount != 1 {
		t.Errorf("expected 1 comment, got %d", commentCount)
	}
}
