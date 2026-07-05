package migration_test

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/google/uuid"
	temporalclient "go.temporal.io/sdk/client"
	temporalworker "go.temporal.io/sdk/worker"

	"github.com/koriebruh/Jengine/internal/audit"
	"github.com/koriebruh/Jengine/internal/cases/migration"
	caseworkflow "github.com/koriebruh/Jengine/internal/cases/workflow"
	"github.com/koriebruh/Jengine/internal/storage/postgres"
	"github.com/koriebruh/Jengine/internal/tenancy"
	"github.com/koriebruh/Jengine/internal/testutil"
)

const localTemporalHostPort = "localhost:7233"

// requireLocalTemporal mirrors this codebase's established
// "skip if the local dev-stack dependency isn't reachable" convention
// (e.g. internal/ingestion's requireLocalRedpanda) - the backfill
// program needs a real client.Client, which testsuite's mocked
// environment doesn't provide (that's for workflow-logic unit tests,
// see workflow_test.go), so this integration test targets the actual
// `make dev-up`-provisioned Temporal dev server (plans/task/core/02).
func requireLocalTemporal(t *testing.T) {
	t.Helper()
	conn, err := net.DialTimeout("tcp", localTemporalHostPort, 2*time.Second)
	if err != nil {
		t.Skipf("local Temporal not reachable at %s (run `make dev-up`): %v", localTemporalHostPort, err)
	}
	_ = conn.Close()
}

func TestBackfill_StartsWorkflowsForOpenCasesOnly(t *testing.T) {
	if testing.Short() {
		t.Skip("requires Docker - skipped under -short; run make test-integration")
	}
	requireLocalTemporal(t)

	db := testutil.StartPostgres(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	tenantID := uuid.New()
	if _, err := db.Pool.Exec(ctx,
		`INSERT INTO tenants (id, name, isolation_tier, region, status) VALUES ($1, 'Acme', 'STANDARD', 'us-east', 'ACTIVE')`,
		tenantID,
	); err != nil {
		t.Fatalf("seed tenant failed: %v", err)
	}
	accountID := uuid.New()
	if _, err := db.Pool.Exec(ctx,
		`INSERT INTO accounts (id, tenant_id, external_account_ref, account_type, currency, name) VALUES ($1, $2, $3, 'BANK', 'USD', 'Account')`,
		accountID, tenantID, accountID.String(),
	); err != nil {
		t.Fatalf("seed account failed: %v", err)
	}

	insertCase := func(status string, assignedTo *string) uuid.UUID {
		t.Helper()
		id := uuid.New()
		if _, err := db.Pool.Exec(ctx,
			`INSERT INTO cases (id, tenant_id, account_id, related_transaction_ids, break_type, status, priority, assigned_to, opened_at)
			 VALUES ($1, $2, $3, '{}', 'UNMATCHED', $4, 'MEDIUM', $5, now())`,
			id, tenantID, accountID, status, assignedTo,
		); err != nil {
			t.Fatalf("seed case (status=%s) failed: %v", status, err)
		}
		return id
	}
	analyst := "analyst-existing"
	openCaseID := insertCase("OPEN", nil)
	inProgressCaseID := insertCase("IN_PROGRESS", &analyst)
	resolvedCaseID := insertCase("RESOLVED", &analyst) // must NOT get a workflow

	appPool := testutil.AppRolePool(t, ctx, db.DSN)
	defer appPool.Close()

	temporalClient, err := temporalclient.Dial(temporalclient.Options{HostPort: localTemporalHostPort})
	if err != nil {
		t.Fatalf("dial temporal failed: %v", err)
	}
	defer temporalClient.Close()

	taskQueue := "backfill-test-" + uuid.NewString()[:8]
	activities := &caseworkflow.Activities{
		TxRunner: func(ctx context.Context, tid uuid.UUID, fn func(ctx context.Context) error) error {
			return postgres.WithTx(tenancy.WithTenant(ctx, tenancy.TenantContext{TenantID: tid}), appPool, tid, fn)
		},
		Cases: postgres.NewCaseRepo(), Audit: audit.NewPostgresWriter(), Routing: postgres.NewCaseRoutingConfigRepo(),
		InsertOutbox: func(ctx context.Context, tenantID, aggregateID uuid.UUID, eventType, topic string, payload []byte) error {
			return nil // not under test here
		},
	}
	w := temporalworker.New(temporalClient, taskQueue, temporalworker.Options{})
	w.RegisterWorkflow(caseworkflow.BreakLifecycleWorkflow)
	w.RegisterWorkflow(caseworkflow.ApprovalWorkflow)
	w.RegisterActivity(activities)
	if err := w.Start(); err != nil {
		t.Fatalf("start worker failed: %v", err)
	}
	defer w.Stop()

	result, err := migration.Run(ctx, db.Pool, appPool, temporalClient, taskQueue)
	if err != nil {
		t.Fatalf("migration.Run failed: %v", err)
	}
	if result.Considered != 2 {
		t.Errorf("expected 2 candidates considered (OPEN + IN_PROGRESS, not RESOLVED), got %d", result.Considered)
	}
	if result.Started != 2 {
		t.Errorf("expected 2 workflows started, got %d (failed: %+v)", result.Started, result.Failed)
	}

	time.Sleep(2 * time.Second) // let the started workflows reach their first checkpoint

	var openWorkflowID, inProgressWorkflowID, resolvedWorkflowID *string
	if err := db.Pool.QueryRow(ctx, `SELECT temporal_workflow_id FROM cases WHERE id = $1`, openCaseID).Scan(&openWorkflowID); err != nil {
		t.Fatalf("query open case failed: %v", err)
	}
	if err := db.Pool.QueryRow(ctx, `SELECT temporal_workflow_id FROM cases WHERE id = $1`, inProgressCaseID).Scan(&inProgressWorkflowID); err != nil {
		t.Fatalf("query in-progress case failed: %v", err)
	}
	if err := db.Pool.QueryRow(ctx, `SELECT temporal_workflow_id FROM cases WHERE id = $1`, resolvedCaseID).Scan(&resolvedWorkflowID); err != nil {
		t.Fatalf("query resolved case failed: %v", err)
	}

	if openWorkflowID == nil || *openWorkflowID != caseworkflow.WorkflowID(openCaseID) {
		t.Errorf("expected open case to get workflow id %s, got %v", caseworkflow.WorkflowID(openCaseID), openWorkflowID)
	}
	if inProgressWorkflowID == nil || *inProgressWorkflowID != caseworkflow.WorkflowID(inProgressCaseID) {
		t.Errorf("expected in-progress case to get workflow id %s, got %v", caseworkflow.WorkflowID(inProgressCaseID), inProgressWorkflowID)
	}
	if resolvedWorkflowID != nil {
		t.Errorf("expected resolved case to keep temporal_workflow_id NULL (plans/task/core/20: never backfill terminal cases), got %v", *resolvedWorkflowID)
	}

	// The already-assigned IN_PROGRESS case must NOT have been
	// re-assigned - assigned_to should still read "analyst-existing".
	var inProgressAssignee *string
	if err := db.Pool.QueryRow(ctx, `SELECT assigned_to FROM cases WHERE id = $1`, inProgressCaseID).Scan(&inProgressAssignee); err != nil {
		t.Fatalf("query in-progress case assignee failed: %v", err)
	}
	if inProgressAssignee == nil || *inProgressAssignee != analyst {
		t.Errorf("expected backfilled IN_PROGRESS case to keep its existing assignee %q, got %v", analyst, inProgressAssignee)
	}
}
