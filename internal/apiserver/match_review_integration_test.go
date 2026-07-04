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
	"github.com/koriebruh/Jengine/internal/domain"
	"github.com/koriebruh/Jengine/internal/storage/postgres"
	"github.com/koriebruh/Jengine/internal/tenancy"
	"github.com/koriebruh/Jengine/internal/testutil"
)

func seedTransactionForReview(t *testing.T, ctx context.Context, db *testutil.TestDB, tenantID, accountID uuid.UUID, ref, amount string) uuid.UUID {
	t.Helper()
	id := uuid.New()
	if _, err := db.Pool.Exec(ctx,
		`INSERT INTO transactions (id, tenant_id, account_id, external_ref, amount, currency, base_amount, value_date, booking_date, side, source_mode, ingestion_idempotency_key, status)
		 VALUES ($1, $2, $3, $4, $5, 'USD', $5, now(), now(), 'DEBIT', 'BATCH', $6, 'PARTIALLY_MATCHED')`,
		id, tenantID, accountID, ref, amount, id.String(),
	); err != nil {
		t.Fatalf("seed transaction failed: %v", err)
	}
	return id
}

// TestMatchReviewService_ListConfirm is plans/task/core/15's DoD
// integration test: ListSuggestedMatches -> ConfirmMatch ->
// Transaction.status updated.
func TestMatchReviewService_ListConfirm(t *testing.T) {
	if testing.Short() {
		t.Skip("requires Docker - skipped under -short; run make test-integration")
	}

	db := testutil.StartPostgres(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	tenantID, accountA, accountB := seedTenantWithTwoAccounts(t, ctx, db)
	appPool := testutil.AppRolePool(t, ctx, db.DSN)
	defer appPool.Close()
	tenantCtx := tenancy.WithTenant(ctx, tenancy.TenantContext{TenantID: tenantID})

	srcTx := seedTransactionForReview(t, ctx, db, tenantID, accountA, "REF-1", "100.00")
	tgtTx := seedTransactionForReview(t, ctx, db, tenantID, accountB, "REF-1", "100.00")

	matchResultRepo := postgres.NewMatchResultRepo()
	var matchResultID uuid.UUID
	if err := postgres.WithTx(tenantCtx, appPool, tenantID, func(ctx context.Context) error {
		result, err := matchResultRepo.Create(ctx, tenantID, domain.MatchResult{
			MatchType: domain.MatchCardinalityOneToOne, ConfidenceScore: decimal.RequireFromString("0.75"),
			Status: domain.MatchResultStatusSuggested, MatchedAt: time.Now(),
		}, []domain.MatchResultLine{
			{TransactionID: srcTx, TenantID: tenantID, Side: domain.MatchResultLineSideSource, AllocatedAmount: decimal.RequireFromString("100.00")},
			{TransactionID: tgtTx, TenantID: tenantID, Side: domain.MatchResultLineSideTarget, AllocatedAmount: decimal.RequireFromString("100.00")},
		})
		matchResultID = result.ID
		return err
	}); err != nil {
		t.Fatalf("seed match result failed: %v", err)
	}

	h := &apiserver.MatchReviewServiceHandler{
		Pool: appPool, MatchResults: matchResultRepo, Transactions: postgres.NewTransactionRepo(),
		Lifecycle: cases.NewPostgresLifecycleService(
			func(ctx context.Context, tid uuid.UUID, fn func(ctx context.Context) error) error {
				return postgres.WithTx(tenancy.WithTenant(ctx, tenancy.TenantContext{TenantID: tid}), appPool, tid, fn)
			}, postgres.NewCaseRepo(), audit.NewPostgresWriter()),
		Audit: audit.NewPostgresWriter(), Idempotency: apiserver.NewPostgresIdempotencyStore(appPool),
	}

	listResp, err := h.ListSuggestedMatches(tenantCtx, connect.NewRequest(&jenginev1.ListSuggestedMatchesRequest{}))
	if err != nil {
		t.Fatalf("ListSuggestedMatches failed: %v", err)
	}
	if len(listResp.Msg.Matches) != 1 {
		t.Fatalf("expected 1 suggested match, got %d", len(listResp.Msg.Matches))
	}
	if listResp.Msg.Matches[0].Id != matchResultID.String() {
		t.Errorf("expected match id %s, got %s", matchResultID, listResp.Msg.Matches[0].Id)
	}

	confirmReq := connect.NewRequest(&jenginev1.ConfirmMatchRequest{Id: matchResultID.String(), ConfirmedBy: "analyst-1"})
	confirmReq.Header().Set("Idempotency-Key", uuid.New().String())
	if _, err := h.ConfirmMatch(tenantCtx, confirmReq); err != nil {
		t.Fatalf("ConfirmMatch failed: %v", err)
	}

	var srcStatus, tgtStatus string
	if err := db.Pool.QueryRow(ctx, `SELECT status FROM transactions WHERE id = $1`, srcTx).Scan(&srcStatus); err != nil {
		t.Fatalf("query srcTx status failed: %v", err)
	}
	if err := db.Pool.QueryRow(ctx, `SELECT status FROM transactions WHERE id = $1`, tgtTx).Scan(&tgtStatus); err != nil {
		t.Fatalf("query tgtTx status failed: %v", err)
	}
	if srcStatus != "MATCHED" || tgtStatus != "MATCHED" {
		t.Errorf("expected both transactions MATCHED after confirm, got src=%s tgt=%s", srcStatus, tgtStatus)
	}
}
