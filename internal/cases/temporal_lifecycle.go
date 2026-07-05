package cases

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"go.temporal.io/sdk/client"

	"github.com/koriebruh/Jengine/internal/cases/workflow"
	"github.com/koriebruh/Jengine/internal/domain"
	"github.com/koriebruh/Jengine/internal/tenancy"
)

// TemporalLifecycleService is plans/task/core/20's Temporal-backed
// LifecycleService implementation - satisfies the EXACT SAME interface
// task 13's PostgresLifecycleService does (this task's own "Interface
// preservation" requirement), so no caller (task 12's BreakSinkAdapter,
// task 15's API layer) needs to change. Everything past the initial
// Break row creation routes through Signals to a per-Break
// BreakLifecycleWorkflow rather than direct Postgres mutation - side
// effects happen inside that workflow's Activities
// (internal/cases/workflow), never here.
type TemporalLifecycleService struct {
	Client    client.Client
	TaskQueue string
	TxRunner  TxRunner
	Cases     domain.CaseRepository
}

func NewTemporalLifecycleService(c client.Client, taskQueue string, txRunner TxRunner, cases domain.CaseRepository) *TemporalLifecycleService {
	return &TemporalLifecycleService{Client: c, TaskQueue: taskQueue, TxRunner: txRunner, Cases: cases}
}

func toWorkflowActor(a Actor) workflow.Actor {
	return workflow.Actor{UserID: a.UserID, Role: a.Role}
}

// OpenBreak creates the Case row directly (there's no workflow yet to
// signal - Temporal workflows operate on an EXISTING entity ID, they
// don't invent one) and starts BreakLifecycleWorkflow with a
// deterministic workflow ID derived from the new Case's ID, matching
// PostgresLifecycleService.OpenBreak's own persistence shape exactly so
// the two implementations produce identical Case rows.
func (s *TemporalLifecycleService) OpenBreak(ctx context.Context, params OpenBreakParams) (domain.Case, error) {
	var result domain.Case
	err := s.TxRunner(ctx, params.TenantID, func(ctx context.Context) error {
		c := domain.Case{
			TenantID: params.TenantID, AccountID: params.AccountID,
			RelatedTransactionIDs: params.TransactionIDs, BreakType: domain.BreakType(params.BreakType),
			Status: BreakOpen, Priority: "MEDIUM",
			AmountAtRisk: &params.AmountAtRisk, Currency: &params.Currency,
		}
		created, err := s.Cases.Create(ctx, params.TenantID, c)
		if err != nil {
			return fmt.Errorf("cases: temporal: open break: %w", err)
		}
		result = created
		return nil
	})
	if err != nil {
		return domain.Case{}, err
	}

	workflowID := workflow.WorkflowID(result.ID)
	_, err = s.Client.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID: workflowID, TaskQueue: s.TaskQueue,
	}, workflow.BreakLifecycleWorkflow, workflow.BreakLifecycleWorkflowInput{
		BreakID: result.ID, TenantID: params.TenantID, InitialStatus: BreakOpen, OpenedAt: result.OpenedAt,
	})
	if err != nil {
		return domain.Case{}, fmt.Errorf("cases: temporal: start BreakLifecycleWorkflow: %w", err)
	}

	if err := s.TxRunner(ctx, params.TenantID, func(ctx context.Context) error {
		return s.Cases.UpdateTemporalWorkflowID(ctx, params.TenantID, result.ID, workflowID)
	}); err != nil {
		return domain.Case{}, fmt.Errorf("cases: temporal: persist workflow id: %w", err)
	}
	result.TemporalWorkflowID = &workflowID
	return result, nil
}

func (s *TemporalLifecycleService) Assign(ctx context.Context, breakID uuid.UUID, assignee string, actor Actor) error {
	return s.Client.SignalWorkflow(ctx, workflow.WorkflowID(breakID), "", workflow.SignalAssign, workflow.AssignSignal{
		Assignee: assignee, Actor: toWorkflowActor(actor),
	})
}

// Transition sends the generic SignalTransition, carrying the target
// status explicitly - covers any valid transition the more specific
// named signals (assign, escalate, resolve, reopen) don't, notably
// ASSIGNED -> IN_PROGRESS (plans/docs/05-case-management.md §6.1's own
// lifecycle diagram has no dedicated action name for that step). The
// workflow's own isValidTransition check rejects anything not in the
// ported transition table - this method doesn't duplicate that
// validation client-side, the signal handler is authoritative.
func (s *TemporalLifecycleService) Transition(ctx context.Context, breakID uuid.UUID, to BreakStatus, actor Actor, comment string) error {
	return s.Client.SignalWorkflow(ctx, workflow.WorkflowID(breakID), "", workflow.SignalTransition, workflow.GenericTransitionSignal{
		To: to, Actor: toWorkflowActor(actor), Comment: comment,
	})
}

func (s *TemporalLifecycleService) AddComment(ctx context.Context, breakID uuid.UUID, actor Actor, body string) (domain.CaseComment, error) {
	if err := s.Client.SignalWorkflow(ctx, workflow.WorkflowID(breakID), "", workflow.SignalComment, workflow.CommentSignal{
		Actor: toWorkflowActor(actor), Body: body,
	}); err != nil {
		return domain.CaseComment{}, fmt.Errorf("cases: temporal: signal comment: %w", err)
	}
	// The comment row itself is written by PersistTransitionActivity
	// (async, inside the workflow) - this method can't return the
	// created domain.CaseComment synchronously the way
	// PostgresLifecycleService.AddComment does, since the write hasn't
	// happened yet by the time SignalWorkflow returns. Callers needing
	// the comment row itself should re-list via
	// domain.CaseRepository.ListComments after the signal is processed
	// (workflows process signals promptly, but not synchronously with
	// the SignalWorkflow call).
	return domain.CaseComment{CaseID: breakID, Actor: actor.UserID, EventType: "comment"}, nil
}

func (s *TemporalLifecycleService) RequestApproval(ctx context.Context, breakID uuid.UUID, actor Actor) error {
	return s.Client.SignalWorkflow(ctx, workflow.WorkflowID(breakID), "", workflow.SignalSubmitForApproval, workflow.SubmitForApprovalSignal{
		Actor: toWorkflowActor(actor), TargetStatus: domain.CaseStatusResolved,
	})
}

// DecideApproval signals the CHILD ApprovalWorkflow directly (its
// deterministic ID is derived from breakID, same derivation
// BreakLifecycleWorkflow itself uses internally to start it) - not the
// parent BreakLifecycleWorkflow, since the parent is BLOCKED waiting on
// the child's result while PENDING_APPROVAL (see
// internal/cases/workflow's runApproval).
func (s *TemporalLifecycleService) DecideApproval(ctx context.Context, breakID uuid.UUID, approver Actor, approve bool, comment string) error {
	return s.Client.SignalWorkflow(ctx, workflow.ApprovalWorkflowID(breakID), "", workflow.SignalApprove, workflow.ApproveRejectSignal{
		ApproverUserID: approver.UserID, ApproverRole: approver.Role, Approve: approve, Comment: comment,
	})
}

// TagRootCause doesn't change lifecycle STATE (BreakStatus is
// untouched), so it's a direct Postgres write here too, matching
// PostgresLifecycleService's own equivalent - not every case action
// needs to route through the workflow, only actual state transitions
// and the side effects (SLA timers, approval gates) tied to them.
func (s *TemporalLifecycleService) TagRootCause(ctx context.Context, breakID uuid.UUID, category string, actor Actor) error {
	return s.TxRunner(ctx, tenancy.MustTenantFromContext(ctx).TenantID, func(ctx context.Context) error {
		tenantID := tenancy.MustTenantFromContext(ctx).TenantID
		if !IsValidRootCause(category) {
			return fmt.Errorf("cases: temporal: tag root cause: unrecognized category %q", category)
		}
		if err := s.Cases.UpdateRootCause(ctx, tenantID, breakID, category); err != nil {
			return fmt.Errorf("cases: temporal: tag root cause: %w", err)
		}
		_, err := s.Cases.AddAuditEvent(ctx, tenantID, domain.CaseAuditEvent{
			CaseID: breakID, Actor: actor.UserID, EventType: "break.root_cause_tagged",
			Payload: mustJSON(map[string]string{"category": category}),
		})
		return err
	})
}

func (s *TemporalLifecycleService) BulkAssign(ctx context.Context, breakIDs []uuid.UUID, assignee string, actor Actor) (BulkResult, error) {
	return s.bulkSignal(ctx, breakIDs, actor, "break.bulk_assigned", func(id uuid.UUID) error {
		return s.Assign(ctx, id, assignee, actor)
	})
}

func (s *TemporalLifecycleService) BulkTransition(ctx context.Context, breakIDs []uuid.UUID, to BreakStatus, actor Actor, comment string) (BulkResult, error) {
	return s.bulkSignal(ctx, breakIDs, actor, "break.bulk_transitioned", func(id uuid.UUID) error {
		return s.Transition(ctx, id, to, actor, comment)
	})
}

func (s *TemporalLifecycleService) BulkAddComment(ctx context.Context, breakIDs []uuid.UUID, actor Actor, body string) (BulkResult, error) {
	return s.bulkSignal(ctx, breakIDs, actor, "break.bulk_commented", func(id uuid.UUID) error {
		_, err := s.AddComment(ctx, id, actor, body)
		return err
	})
}

// bulkSignal mirrors PostgresLifecycleService.bulkOp's per-ID success/
// failure tracking and single batch-level audit event
// (plans/docs/05-case-management.md §6.2: "single audit event
// referencing batch-op id + affected case ids"), but loops over
// SignalWorkflow calls (fire-and-forget per ID, no shared transaction
// needed the way Postgres writes do) instead of repository writes.
func (s *TemporalLifecycleService) bulkSignal(ctx context.Context, breakIDs []uuid.UUID, actor Actor, eventType string, perID func(id uuid.UUID) error) (BulkResult, error) {
	result := BulkResult{BatchOpID: uuid.New(), Failed: make(map[uuid.UUID]string)}
	if len(breakIDs) == 0 {
		return result, nil
	}
	for _, id := range breakIDs {
		if err := perID(id); err != nil {
			result.Failed[id] = err.Error()
			continue
		}
		result.Succeeded = append(result.Succeeded, id)
	}
	if len(result.Succeeded) == 0 {
		return result, nil // nothing to audit if every item failed
	}

	tenantID := tenancy.MustTenantFromContext(ctx).TenantID
	return result, s.TxRunner(ctx, tenantID, func(ctx context.Context) error {
		payload := mustJSON(map[string]any{
			"batch_op_id": result.BatchOpID, "break_ids": result.Succeeded, "failed_count": len(result.Failed),
		})
		// case_audit_events.case_id is NOT NULL/FK - one row per batch
		// referencing the FIRST succeeded break as its case_id (same
		// convention PostgresLifecycleService.bulkOp uses); batch_op_id
		// in the payload is the real correlator across the whole batch,
		// not case_id.
		_, err := s.Cases.AddAuditEvent(ctx, tenantID, domain.CaseAuditEvent{
			CaseID: result.Succeeded[0], Actor: actor.UserID, EventType: eventType, Payload: payload,
		})
		return err
	})
}

var _ LifecycleService = (*TemporalLifecycleService)(nil)
