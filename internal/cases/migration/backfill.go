// Package migration implements plans/task/core/20's one-time backfill:
// starting a BreakLifecycleWorkflow for every existing open Break row
// that doesn't have one yet, so already-in-flight cases get the same
// durable SLA timers/Signal-driven lifecycle newly-created ones do.
package migration

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/client"

	"github.com/koriebruh/Jengine/internal/cases/workflow"
	"github.com/koriebruh/Jengine/internal/domain"
	"github.com/koriebruh/Jengine/internal/storage/postgres"
	"github.com/koriebruh/Jengine/internal/tenancy"
)

// candidateRow is one open Break row needing a workflow started -
// enough fields to construct BreakLifecycleWorkflowInput without a
// second per-row query.
type candidateRow struct {
	id       uuid.UUID
	tenantID uuid.UUID
	status   domain.CaseStatus
	openedAt time.Time
	slaDueAt *time.Time
	assignee *string
}

// Result summarizes one backfill run - printed by whatever caller
// invokes Run (e.g. a one-off `go run` command; this package doesn't
// define its own cmd/ binary per plans/task/core/20's own topology
// note that everything Temporal-related here lives inside cmd/coreapi,
// not a new binary).
type Result struct {
	Considered int
	Started    int
	Failed     map[uuid.UUID]string
}

// Run selects every Break row where temporal_workflow_id IS NULL AND
// status NOT IN (RESOLVED, WRITTEN_OFF) - plans/task/core/20
// Implementation Notes: only open cases need a live workflow; resolved/
// written-off historical rows keep temporal_workflow_id = NULL
// permanently (cheaper, replaying closed-case history isn't required).
// For each, starts BreakLifecycleWorkflow with InitialStatus set to the
// row's CURRENT status (never OPEN, unless that genuinely is the
// current status) - carrying existing assigned_to/sla_due_at through so
// the workflow doesn't redo work a human already did (this task's own
// Common Pitfalls: re-running AutoAssignActivity for an already-
// assigned case "is a real regression a user would notice
// immediately"). Idempotent: the deterministic workflow ID
// (workflow.WorkflowID) makes re-running this program on rows a
// previous run already handled a safe no-op (Temporal rejects starting
// a workflow with an ID that already exists as a WorkflowExecutionAlreadyStarted
// error, which this function treats as success, not failure - see the
// loop below), not a duplicate start.
func Run(ctx context.Context, superuserPool *pgxpool.Pool, appPool *pgxpool.Pool, temporalClient client.Client, taskQueue string) (Result, error) {
	result := Result{Failed: make(map[uuid.UUID]string)}

	rows, err := superuserPool.Query(ctx,
		`SELECT id, tenant_id, status, opened_at, sla_due_at, assigned_to
		 FROM cases WHERE temporal_workflow_id IS NULL AND status NOT IN ('RESOLVED', 'WRITTEN_OFF')`,
	)
	if err != nil {
		return result, fmt.Errorf("migration: query candidate cases: %w", err)
	}
	defer rows.Close()

	var candidates []candidateRow
	for rows.Next() {
		var c candidateRow
		if err := rows.Scan(&c.id, &c.tenantID, &c.status, &c.openedAt, &c.slaDueAt, &c.assignee); err != nil {
			return result, fmt.Errorf("migration: scan candidate case: %w", err)
		}
		candidates = append(candidates, c)
	}
	if err := rows.Err(); err != nil {
		return result, fmt.Errorf("migration: iterate candidate cases: %w", err)
	}
	result.Considered = len(candidates)

	caseRepo := postgres.NewCaseRepo()
	for _, c := range candidates {
		assignee := ""
		if c.assignee != nil {
			assignee = *c.assignee
		}
		workflowID := workflow.WorkflowID(c.id)

		_, startErr := temporalClient.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
			ID: workflowID, TaskQueue: taskQueue,
		}, workflow.BreakLifecycleWorkflow, workflow.BreakLifecycleWorkflowInput{
			BreakID: c.id, TenantID: c.tenantID, InitialStatus: c.status,
			OpenedAt: c.openedAt, SLADueAt: c.slaDueAt, Assignee: assignee,
		})
		// A workflow already running under this deterministic ID (e.g.
		// a previous backfill run already started it, and only the
		// temporal_workflow_id persist step below failed/was
		// interrupted) is the expected idempotent-resume case, not a
		// failure - proceed to (re-)persist the ID rather than
		// reporting an error for it.
		var alreadyStarted *serviceerror.WorkflowExecutionAlreadyStarted
		if startErr != nil && !errors.As(startErr, &alreadyStarted) {
			result.Failed[c.id] = startErr.Error()
			continue
		}

		tenantCtx := tenancy.WithTenant(ctx, tenancy.TenantContext{TenantID: c.tenantID})
		if txErr := postgres.WithTx(tenantCtx, appPool, c.tenantID, func(ctx context.Context) error {
			return caseRepo.UpdateTemporalWorkflowID(ctx, c.tenantID, c.id, workflowID)
		}); txErr != nil {
			result.Failed[c.id] = fmt.Sprintf("workflow started but failed to persist workflow id: %v", txErr)
			continue
		}
		result.Started++
	}

	return result, nil
}
