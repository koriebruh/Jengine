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

// TestMatchReviewService_BulkConfirmMatches_MixedSelectionPartiallyFails
// is plans/task/core/15's DoD test for bulk endpoints: a mixed-status
// selection produces a per-ID success/failure breakdown, and exactly one
// audit event is written for the whole batch, not one per item.
func TestMatchReviewService_BulkConfirmMatches_MixedSelectionPartiallyFails(t *testing.T) {
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

	matchResultRepo := postgres.NewMatchResultRepo()

	seedSuggestedMatch := func() uuid.UUID {
		srcTx := seedTransactionForReview(t, ctx, db, tenantID, accountA, uuid.NewString(), "10.00")
		tgtTx := seedTransactionForReview(t, ctx, db, tenantID, accountB, uuid.NewString(), "10.00")
		var resultID uuid.UUID
		if err := postgres.WithTx(tenantCtx, appPool, tenantID, func(ctx context.Context) error {
			result, err := matchResultRepo.Create(ctx, tenantID, domain.MatchResult{
				MatchType: domain.MatchCardinalityOneToOne, ConfidenceScore: decimal.RequireFromString("0.8"),
				Status: domain.MatchResultStatusSuggested, MatchedAt: time.Now(),
			}, []domain.MatchResultLine{
				{TransactionID: srcTx, TenantID: tenantID, Side: domain.MatchResultLineSideSource, AllocatedAmount: decimal.RequireFromString("10.00")},
				{TransactionID: tgtTx, TenantID: tenantID, Side: domain.MatchResultLineSideTarget, AllocatedAmount: decimal.RequireFromString("10.00")},
			})
			resultID = result.ID
			return err
		}); err != nil {
			t.Fatalf("seed match result failed: %v", err)
		}
		return resultID
	}

	validA := seedSuggestedMatch()
	validB := seedSuggestedMatch()
	invalidID := uuid.New() // doesn't exist -> must fail, not abort the whole batch

	h := &apiserver.MatchReviewServiceHandler{
		Pool: appPool, MatchResults: matchResultRepo, Transactions: postgres.NewTransactionRepo(),
		Lifecycle: cases.NewPostgresLifecycleService(
			func(ctx context.Context, tid uuid.UUID, fn func(ctx context.Context) error) error {
				return postgres.WithTx(tenancy.WithTenant(ctx, tenancy.TenantContext{TenantID: tid}), appPool, tid, fn)
			}, postgres.NewCaseRepo(), audit.NewPostgresWriter()),
		Audit: audit.NewPostgresWriter(), Idempotency: apiserver.NewPostgresIdempotencyStore(appPool),
	}

	req := connect.NewRequest(&jenginev1.BulkConfirmMatchesRequest{
		Ids: []string{validA.String(), validB.String(), invalidID.String()}, ConfirmedBy: "analyst-1",
	})
	req.Header().Set("Idempotency-Key", uuid.New().String())

	resp, err := h.BulkConfirmMatches(tenantCtx, req)
	if err != nil {
		t.Fatalf("BulkConfirmMatches returned an unexpected top-level error: %v", err)
	}

	if len(resp.Msg.Result.Succeeded) != 2 {
		t.Errorf("expected 2 succeeded IDs, got %d: %v", len(resp.Msg.Result.Succeeded), resp.Msg.Result.Succeeded)
	}
	if len(resp.Msg.Result.Failed) != 1 {
		t.Errorf("expected 1 failed ID, got %d: %v", len(resp.Msg.Result.Failed), resp.Msg.Result.Failed)
	}
	if _, failed := resp.Msg.Result.Failed[invalidID.String()]; !failed {
		t.Errorf("expected invalidID (%s) to be reported as failed", invalidID)
	}

	for _, id := range []uuid.UUID{validA, validB} {
		var status string
		if err := db.Pool.QueryRow(ctx, `SELECT status FROM match_results WHERE id = $1`, id).Scan(&status); err != nil {
			t.Fatalf("query match_result status for %s failed: %v", id, err)
		}
		if status != "CONFIRMED" {
			t.Errorf("expected match result %s to be CONFIRMED despite invalidID's failure, got %s", id, status)
		}
	}

	var auditCount int
	if err := db.Pool.QueryRow(ctx,
		`SELECT count(*) FROM audit_events WHERE tenant_id = $1 AND event_type = 'match.bulk_confirmed'`,
		tenantID,
	).Scan(&auditCount); err != nil {
		t.Fatalf("count audit_events failed: %v", err)
	}
	if auditCount != 1 {
		t.Errorf("expected exactly 1 audit event for the whole batch op, got %d", auditCount)
	}
}
