package cases_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"

	"github.com/koriebruh/Jengine/internal/audit"
	"github.com/koriebruh/Jengine/internal/cases"
	"github.com/koriebruh/Jengine/internal/storage/postgres"
	"github.com/koriebruh/Jengine/internal/tenancy"
	"github.com/koriebruh/Jengine/internal/testutil"
)

func seedTenantAndAccount(t *testing.T, ctx context.Context, db *testutil.TestDB) (tenantID, accountID uuid.UUID) {
	t.Helper()
	tenantID, accountID = uuid.New(), uuid.New()
	if _, err := db.Pool.Exec(ctx,
		`INSERT INTO tenants (id, name, isolation_tier, region, status) VALUES ($1, 'Acme', 'STANDARD', 'us-east', 'ACTIVE')`,
		tenantID,
	); err != nil {
		t.Fatalf("seed tenant failed: %v", err)
	}
	if _, err := db.Pool.Exec(ctx,
		`INSERT INTO accounts (id, tenant_id, external_account_ref, account_type, currency, name) VALUES ($1, $2, $3, 'BANK', 'USD', 'Account')`,
		accountID, tenantID, accountID.String(),
	); err != nil {
		t.Fatalf("seed account failed: %v", err)
	}
	return tenantID, accountID
}

func newLifecycleService(t *testing.T, ctx context.Context, db *testutil.TestDB) *cases.PostgresLifecycleService {
	t.Helper()
	appPool := testutil.AppRolePool(t, ctx, db.DSN)
	t.Cleanup(appPool.Close)

	txRunner := func(ctx context.Context, tenantID uuid.UUID, fn func(ctx context.Context) error) error {
		return postgres.WithTx(tenancy.WithTenant(ctx, tenancy.TenantContext{TenantID: tenantID}), appPool, tenantID, fn)
	}
	return cases.NewPostgresLifecycleService(txRunner, postgres.NewCaseRepo(), audit.NewPostgresWriter())
}

func TestDecideApproval_MakerNotEqualCheckerRejected(t *testing.T) {
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

	brk, err := svc.OpenBreak(ctx, cases.OpenBreakParams{
		TenantID: tenantID, AccountID: accountID, TransactionIDs: []uuid.UUID{uuid.New()},
		BreakType: "UNMATCHED", AmountAtRisk: decimal.RequireFromString("100.00"), Currency: "USD",
	})
	if err != nil {
		t.Fatalf("OpenBreak failed: %v", err)
	}
	if err := svc.Assign(tenantCtx, brk.ID, "analyst-1", requester); err != nil {
		t.Fatalf("Assign failed: %v", err)
	}
	if err := svc.Transition(tenantCtx, brk.ID, cases.BreakInProgress, requester, ""); err != nil {
		t.Fatalf("Transition to IN_PROGRESS failed: %v", err)
	}
	if err := svc.RequestApproval(tenantCtx, brk.ID, requester); err != nil {
		t.Fatalf("RequestApproval failed: %v", err)
	}

	// Same actor who requested approval must NOT be allowed to decide it.
	err = svc.DecideApproval(tenantCtx, brk.ID, requester, true, "self-approving")
	if err == nil {
		t.Fatal("expected DecideApproval to reject the same actor as approver (maker == checker)")
	}

	// A DIFFERENT actor deciding must succeed.
	checker := cases.Actor{UserID: "analyst-2", Role: "ANALYST"}
	if err := svc.DecideApproval(tenantCtx, brk.ID, checker, true, "approved by a different analyst"); err != nil {
		t.Fatalf("expected DecideApproval by a different actor to succeed, got: %v", err)
	}
}
