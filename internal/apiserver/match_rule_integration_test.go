package apiserver_test

import (
	"context"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/google/uuid"

	jenginev1 "github.com/koriebruh/Jengine/gen/go/jengine/v1"
	"github.com/koriebruh/Jengine/internal/apiserver"
	"github.com/koriebruh/Jengine/internal/matching/rules"
	"github.com/koriebruh/Jengine/internal/storage/postgres"
	"github.com/koriebruh/Jengine/internal/tenancy"
	"github.com/koriebruh/Jengine/internal/testutil"
)

const testRuleYAML = `
rule:
  name: "Test Rule"
  version: 1
  match_cardinality: ONE_TO_ONE
  keys:
    - field: currency
      tolerance: exact
  scoring:
    - field: reference
      method: exact
      weight: 1.0
  thresholds:
    auto_match: 0.9
    suggest: 0.5
  execution:
    priority: 1
`

// TestMatchRuleService_CreateActivateGet is plans/task/core/15's DoD
// integration test: CreateDraftRule -> ActivateRule -> visible via
// GetRule.
func TestMatchRuleService_CreateActivateGet(t *testing.T) {
	if testing.Short() {
		t.Skip("requires Docker - skipped under -short; run make test-integration")
	}

	db := testutil.StartPostgres(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	tenantID, accountA, accountB := seedTenantWithTwoAccounts(t, ctx, db)
	appPool := testutil.AppRolePool(t, ctx, db.DSN)
	defer appPool.Close()

	h := &apiserver.MatchRuleServiceHandler{
		Pool: appPool, Rules: postgres.NewMatchRuleRepo(), Registry: rules.DefaultRegistry(),
		Idempotency: apiserver.NewPostgresIdempotencyStore(appPool),
	}
	tenantCtx := tenancy.WithTenant(ctx, tenancy.TenantContext{TenantID: tenantID})

	createReq := connect.NewRequest(&jenginev1.CreateDraftRuleRequest{
		RuleSpecYaml: testRuleYAML, SourceAccountId: accountA.String(), TargetAccountId: accountB.String(),
	})
	createReq.Header().Set("Idempotency-Key", uuid.New().String())

	createResp, err := h.CreateDraftRule(tenantCtx, createReq)
	if err != nil {
		t.Fatalf("CreateDraftRule failed: %v", err)
	}
	if createResp.Msg.Rule.Status != "DRAFT" {
		t.Fatalf("expected DRAFT status, got %s", createResp.Msg.Rule.Status)
	}
	ruleID := createResp.Msg.Rule.Id

	activateReq := connect.NewRequest(&jenginev1.ActivateRuleRequest{Id: ruleID, ApprovedBy: "different-user"})
	activateReq.Header().Set("Idempotency-Key", uuid.New().String())
	activateResp, err := h.ActivateRule(tenantCtx, activateReq)
	if err != nil {
		t.Fatalf("ActivateRule failed: %v", err)
	}
	if activateResp.Msg.Rule.Status != "ACTIVE" {
		t.Fatalf("expected ACTIVE status after activation, got %s", activateResp.Msg.Rule.Status)
	}

	getResp, err := h.GetRule(tenantCtx, connect.NewRequest(&jenginev1.GetRuleRequest{Id: ruleID}))
	if err != nil {
		t.Fatalf("GetRule failed: %v", err)
	}
	if getResp.Msg.Rule.Status != "ACTIVE" {
		t.Errorf("expected GetRule to show ACTIVE, got %s", getResp.Msg.Rule.Status)
	}
	if getResp.Msg.Rule.ApprovedBy != "different-user" {
		t.Errorf("expected approved_by 'different-user', got %q", getResp.Msg.Rule.ApprovedBy)
	}
}

// TestMatchRuleService_ActivateRule_SameApproverAsCreatorRejected is
// plans/task/core/15's maker-checker-lite requirement.
func TestMatchRuleService_ActivateRule_SameApproverAsCreatorRejected(t *testing.T) {
	if testing.Short() {
		t.Skip("requires Docker - skipped under -short; run make test-integration")
	}

	db := testutil.StartPostgres(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	tenantID, accountA, accountB := seedTenantWithTwoAccounts(t, ctx, db)
	appPool := testutil.AppRolePool(t, ctx, db.DSN)
	defer appPool.Close()

	h := &apiserver.MatchRuleServiceHandler{
		Pool: appPool, Rules: postgres.NewMatchRuleRepo(), Registry: rules.DefaultRegistry(),
		Idempotency: apiserver.NewPostgresIdempotencyStore(appPool),
	}
	tenantCtx := tenancy.WithTenant(ctx, tenancy.TenantContext{TenantID: tenantID})

	createReq := connect.NewRequest(&jenginev1.CreateDraftRuleRequest{
		RuleSpecYaml: testRuleYAML, SourceAccountId: accountA.String(), TargetAccountId: accountB.String(),
	})
	createReq.Header().Set("Idempotency-Key", uuid.New().String())
	createResp, err := h.CreateDraftRule(tenantCtx, createReq)
	if err != nil {
		t.Fatalf("CreateDraftRule failed: %v", err)
	}
	// created_by is hardcoded "api" in the handler at MVP (see
	// match_rule_service.go) - approving as the same "api" actor must be
	// rejected.
	activateReq := connect.NewRequest(&jenginev1.ActivateRuleRequest{Id: createResp.Msg.Rule.Id, ApprovedBy: "api"})
	activateReq.Header().Set("Idempotency-Key", uuid.New().String())
	_, err = h.ActivateRule(tenantCtx, activateReq)
	if err == nil {
		t.Fatal("expected ActivateRule to reject approved_by == created_by")
	}
}

func seedTenantWithTwoAccounts(t *testing.T, ctx context.Context, db *testutil.TestDB) (tenantID, accountA, accountB uuid.UUID) {
	t.Helper()
	tenantID, accountA, accountB = uuid.New(), uuid.New(), uuid.New()
	if _, err := db.Pool.Exec(ctx,
		`INSERT INTO tenants (id, name, isolation_tier, region, status) VALUES ($1, 'Acme', 'STANDARD', 'us-east', 'ACTIVE')`,
		tenantID,
	); err != nil {
		t.Fatalf("seed tenant failed: %v", err)
	}
	for _, id := range []uuid.UUID{accountA, accountB} {
		if _, err := db.Pool.Exec(ctx,
			`INSERT INTO accounts (id, tenant_id, external_account_ref, account_type, currency, name) VALUES ($1, $2, $3, 'BANK', 'USD', 'Account')`,
			id, tenantID, id.String(),
		); err != nil {
			t.Fatalf("seed account failed: %v", err)
		}
	}
	return tenantID, accountA, accountB
}
