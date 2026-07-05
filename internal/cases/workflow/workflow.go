package workflow

import (
	"fmt"
	"time"

	"github.com/google/uuid"
	"go.temporal.io/sdk/log"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/koriebruh/Jengine/internal/domain"
)

// Signal channel names - plans/task/core/20 Implementation Notes lists
// 8 signals: assign, comment, transition, submit_for_approval, escalate,
// resolve, write_off, reopen. escalate/resolve/reopen are named
// shortcuts for their fixed target status (operational visibility in
// Temporal's UI/history); "transition" is the GENERIC catch-all for any
// OTHER valid transition the fixed-name signals don't cover - notably
// ASSIGNED -> IN_PROGRESS, which has no dedicated action name in the
// design's own lifecycle diagram (plans/docs/05-case-management.md
// §6.1) the way "escalate"/"resolve" do. Found missing during this
// package's own workflow tests: the happy-path test's ASSIGNED ->
// RESOLVED signal silently no-op'd (isValidTransition rejects it,
// IN_PROGRESS is required first) with no way to reach IN_PROGRESS at
// all until this generic signal was added back.
const (
	SignalAssign            = "assign"
	SignalComment           = "comment"
	SignalTransition        = "transition"
	SignalSubmitForApproval = "submit_for_approval"
	SignalEscalate          = "escalate"
	SignalResolve           = "resolve"
	SignalWriteOff          = "write_off"
	SignalReopen            = "reopen"
)

// BreakLifecycleWorkflowInput is BreakLifecycleWorkflow's start
// parameter. SLADueAt is nil for a genuinely new case (ComputeSLAActivity
// computes it); backfill (plans/task/core/20's migration/backfill.go)
// passes the ALREADY-COMPUTED value through so a resumed workflow
// doesn't recompute (and potentially change) an SLA a human is already
// working against.
type BreakLifecycleWorkflowInput struct {
	BreakID       uuid.UUID
	TenantID      uuid.UUID
	InitialStatus domain.CaseStatus // OPEN for new cases; ASSIGNED/IN_PROGRESS/... when resuming via backfill
	OpenedAt      time.Time
	SLADueAt      *time.Time
	// Assignee carries an already-assigned case's current assignee
	// through on backfill/resume - InitialStatus != OPEN means
	// AutoAssignActivity must NOT re-run (plans/task/core/20 Common
	// Pitfalls: "a real regression a user would notice immediately").
	Assignee string
}

// WorkflowID is the deterministic Temporal workflow ID for a Break -
// required for idempotent (re)starts during backfill (starting a
// workflow with an ID that already exists is a no-op/error, not a
// duplicate, which is exactly the idempotency backfill needs).
func WorkflowID(breakID uuid.UUID) string { return "case-" + breakID.String() }

// ApprovalWorkflowID is the deterministic Temporal workflow ID for the
// ApprovalWorkflow child a given Break's PENDING_APPROVAL state starts -
// exported so internal/cases's TemporalLifecycleService.DecideApproval
// (which signals this child directly, not the parent) and this
// package's own tests can derive it without duplicating the "-approval"
// suffix convention inline.
func ApprovalWorkflowID(breakID uuid.UUID) string { return WorkflowID(breakID) + "-approval" }

var defaultActivityOptions = workflow.ActivityOptions{
	StartToCloseTimeout: 30 * time.Second,
	RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 5},
}

// BreakLifecycleWorkflow orchestrates one Break's entire lifecycle -
// durable SLA timers, human-in-the-loop Signals, and maker-checker
// approval gates via the ApprovalWorkflow child workflow
// (plans/task/core/20, plans/docs/05-case-management.md §6.1).
func BreakLifecycleWorkflow(ctx workflow.Context, in BreakLifecycleWorkflowInput) error {
	ctx = workflow.WithActivityOptions(ctx, defaultActivityOptions)
	logger := workflow.GetLogger(ctx)

	status := in.InitialStatus
	assignee := in.Assignee

	var a *Activities // nil receiver: only method VALUES are used via ExecuteActivity by registered name, actual dispatch is via the worker's registered Activities instance, not this pointer - see cmd/coreapi's worker registration.

	if in.InitialStatus == domain.CaseStatusOpen {
		var assignResult AutoAssignResult
		if err := workflow.ExecuteActivity(ctx, a.AutoAssignActivity, AutoAssignInput{
			TenantID: in.TenantID, BreakID: in.BreakID,
		}).Get(ctx, &assignResult); err != nil {
			return fmt.Errorf("workflow: auto-assign: %w", err)
		}
		assignee = assignResult.Assignee

		if err := workflow.ExecuteActivity(ctx, a.PersistTransitionActivity, PersistTransitionInput{
			TenantID: in.TenantID, BreakID: in.BreakID, From: domain.CaseStatusOpen, To: domain.CaseStatusAssigned,
			Actor: Actor{UserID: "system", Role: "SYSTEM"}, Assignee: assignee, EventType: "break.assigned",
		}).Get(ctx, nil); err != nil {
			return fmt.Errorf("workflow: persist initial assignment: %w", err)
		}
		status = domain.CaseStatusAssigned
	}
	// InitialStatus != OPEN (backfill/resume): do NOT re-run
	// AutoAssignActivity - jump straight into the signal-await loop at
	// the resumed status, per this task's own Implementation Notes.

	slaDueAt := in.SLADueAt
	if slaDueAt == nil {
		var slaResult ComputeSLAResult
		if err := workflow.ExecuteActivity(ctx, a.ComputeSLAActivity, ComputeSLAInput{
			TenantID: in.TenantID, OpenedAt: in.OpenedAt, Priority: "MEDIUM",
		}).Get(ctx, &slaResult); err != nil {
			return fmt.Errorf("workflow: compute sla: %w", err)
		}
		slaDueAt = &slaResult.SLADueAt
	}

	// Timer durations computed relative to workflow.Now (deterministic
	// replay clock, not wall-clock time.Now()) - if a resumed/backfilled
	// workflow's checkpoint is already in the past, the duration goes
	// negative; Temporal fires a timer with a non-positive duration
	// immediately, which is exactly the desired behavior for a
	// backfilled case that's already past its SLA warning/breach point.
	warningAt := in.OpenedAt.Add(time.Duration(float64(slaDueAt.Sub(in.OpenedAt)) * 0.75))
	warningTimer := workflow.NewTimer(ctx, warningAt.Sub(workflow.Now(ctx)))
	breachTimer := workflow.NewTimer(ctx, slaDueAt.Sub(workflow.Now(ctx)))

	assignCh := workflow.GetSignalChannel(ctx, SignalAssign)
	commentCh := workflow.GetSignalChannel(ctx, SignalComment)
	transitionCh := workflow.GetSignalChannel(ctx, SignalTransition)
	submitApprovalCh := workflow.GetSignalChannel(ctx, SignalSubmitForApproval)
	escalateCh := workflow.GetSignalChannel(ctx, SignalEscalate)
	resolveCh := workflow.GetSignalChannel(ctx, SignalResolve)
	writeOffCh := workflow.GetSignalChannel(ctx, SignalWriteOff)
	reopenCh := workflow.GetSignalChannel(ctx, SignalReopen)

	isTerminal := func(s domain.CaseStatus) bool {
		return s == domain.CaseStatusResolved || s == domain.CaseStatusWrittenOff
	}

	// warningFired/breachFired guard against re-adding an already-
	// resolved workflow.Future to the Selector on a later loop
	// iteration - a Future stays "ready" forever once resolved, so
	// without this guard, Select() would find it ready on EVERY
	// subsequent iteration and re-run the callback in a tight busy-loop
	// instead of blocking for the next real signal. Caught by reasoning
	// through this package's own SLA-timer test before ever running it
	// (plans/task/core/20 Common Pitfalls doesn't call this out
	// explicitly, but it's the same class of mistake as the pitfalls it
	// does list around Temporal determinism/replay).
	warningFired, breachFired := false, false

	for {
		selector := workflow.NewSelector(ctx)

		if !warningFired {
			selector.AddFuture(warningTimer, func(f workflow.Future) {
				warningFired = true
				if isTerminal(status) {
					return
				}
				payload := mustJSON(map[string]any{"break_id": in.BreakID, "sla_due_at": slaDueAt})
				if err := workflow.ExecuteActivity(ctx, a.EmitOutboxEventActivity, EmitOutboxEventInput{
					TenantID: in.TenantID, AggregateID: in.BreakID, EventType: "break.sla_warning",
					Topic: "case.events.default", Payload: payload,
				}).Get(ctx, nil); err != nil {
					logger.Error("emit sla_warning outbox event failed", "error", err)
				}
			})
		}

		if !breachFired {
			selector.AddFuture(breachTimer, func(f workflow.Future) {
				breachFired = true
				if isTerminal(status) {
					return
				}
				payload := mustJSON(map[string]any{"break_id": in.BreakID, "sla_due_at": slaDueAt})
				if err := workflow.ExecuteActivity(ctx, a.EmitOutboxEventActivity, EmitOutboxEventInput{
					TenantID: in.TenantID, AggregateID: in.BreakID, EventType: "break.sla_breached",
					Topic: "case.events.default", Payload: payload,
				}).Get(ctx, nil); err != nil {
					logger.Error("emit sla_breached outbox event failed", "error", err)
				}
				// Breach: priority bump + escalate, per plans/docs/05-case-management.md
				// §6.3 - only if a valid transition from the current state.
				if domain.CaseStatus(status) != domain.CaseStatusEscalated && isValidTransition(status, domain.CaseStatusEscalated) {
					if err := workflow.ExecuteActivity(ctx, a.PersistTransitionActivity, PersistTransitionInput{
						TenantID: in.TenantID, BreakID: in.BreakID, From: status, To: domain.CaseStatusEscalated,
						Actor: Actor{UserID: "system", Role: "SYSTEM"}, EventType: "break.sla_breach_escalated",
					}).Get(ctx, nil); err != nil {
						logger.Error("persist sla-breach escalation failed", "error", err)
						return
					}
					status = domain.CaseStatusEscalated
				}
			})
		}

		selector.AddReceive(assignCh, func(c workflow.ReceiveChannel, more bool) {
			var sig AssignSignal
			c.Receive(ctx, &sig)
			if !isValidTransition(status, domain.CaseStatusAssigned) {
				logger.Warn("assign signal: invalid transition", "from", status)
				return
			}
			if err := workflow.ExecuteActivity(ctx, a.PersistTransitionActivity, PersistTransitionInput{
				TenantID: in.TenantID, BreakID: in.BreakID, From: status, To: domain.CaseStatusAssigned,
				Actor: sig.Actor, Assignee: sig.Assignee, EventType: "break.assigned",
			}).Get(ctx, nil); err != nil {
				logger.Error("persist assign failed", "error", err)
				return
			}
			status, assignee = domain.CaseStatusAssigned, sig.Assignee
		})

		selector.AddReceive(commentCh, func(c workflow.ReceiveChannel, more bool) {
			var sig CommentSignal
			c.Receive(ctx, &sig)
			if err := workflow.ExecuteActivity(ctx, a.PersistTransitionActivity, PersistTransitionInput{
				TenantID: in.TenantID, BreakID: in.BreakID, From: status, To: status,
				Actor: sig.Actor, Comment: sig.Body, EventType: "break.commented",
			}).Get(ctx, nil); err != nil {
				logger.Error("persist comment failed", "error", err)
			}
		})

		selector.AddReceive(transitionCh, func(c workflow.ReceiveChannel, more bool) {
			var sig GenericTransitionSignal
			c.Receive(ctx, &sig)
			handleTransition(ctx, a, in, &status, sig.To, TransitionSignal{Actor: sig.Actor, Comment: sig.Comment}, "break.transitioned", logger)
		})

		selector.AddReceive(escalateCh, func(c workflow.ReceiveChannel, more bool) {
			var sig TransitionSignal
			c.Receive(ctx, &sig)
			handleTransition(ctx, a, in, &status, domain.CaseStatusEscalated, sig, "break.escalated", logger)
		})

		selector.AddReceive(reopenCh, func(c workflow.ReceiveChannel, more bool) {
			var sig TransitionSignal
			c.Receive(ctx, &sig)
			handleTransition(ctx, a, in, &status, domain.CaseStatusReopened, sig, "break.reopened", logger)
		})

		selector.AddReceive(resolveCh, func(c workflow.ReceiveChannel, more bool) {
			var sig TransitionSignal
			c.Receive(ctx, &sig)
			handleTransition(ctx, a, in, &status, domain.CaseStatusResolved, sig, "break.resolved", logger)
		})

		selector.AddReceive(writeOffCh, func(c workflow.ReceiveChannel, more bool) {
			var sig SubmitForApprovalSignal
			c.Receive(ctx, &sig)
			runApproval(ctx, a, in, &status, domain.CaseStatusWrittenOff, sig, logger)
		})

		selector.AddReceive(submitApprovalCh, func(c workflow.ReceiveChannel, more bool) {
			var sig SubmitForApprovalSignal
			c.Receive(ctx, &sig)
			target := sig.TargetStatus
			if target == "" {
				target = domain.CaseStatusResolved
			}
			runApproval(ctx, a, in, &status, target, sig, logger)
		})

		selector.Select(ctx)
	}
}

// handleTransition validates+persists a plain (non-approval-gated)
// transition - used by escalate/reopen/resolve-without-approval-required
// signal handlers.
func handleTransition(ctx workflow.Context, a *Activities, in BreakLifecycleWorkflowInput, status *domain.CaseStatus, to domain.CaseStatus, sig TransitionSignal, eventType string, logger log.Logger) {
	if requiresApprovalTransition(*status, to) {
		logger.Warn("transition requires approval, use submit_for_approval/write_off instead", "from", *status, "to", to)
		return
	}
	if !isValidTransition(*status, to) {
		logger.Warn("invalid transition", "from", *status, "to", to)
		return
	}
	if err := workflow.ExecuteActivity(ctx, a.PersistTransitionActivity, PersistTransitionInput{
		TenantID: in.TenantID, BreakID: in.BreakID, From: *status, To: to,
		Actor: sig.Actor, Comment: sig.Comment, EventType: eventType,
	}).Get(ctx, nil); err != nil {
		logger.Error("persist transition failed", "error", err)
		return
	}
	*status = to
}

// runApproval starts (and blocks on) the ApprovalWorkflow child, per
// plans/task/core/20 Implementation Notes: "submit_for_approval /
// write_off signals: ExecuteChildWorkflow(ctx, ApprovalWorkflow, ...),
// block on its result before proceeding." Blocks the parent's own
// Selector loop from processing OTHER signals for the duration -
// a deliberate MVP simplification (see this file's package doc for the
// tradeoff), not something later tasks are expected to leave unexamined
// if approval latency becomes a real operational concern.
func runApproval(ctx workflow.Context, a *Activities, in BreakLifecycleWorkflowInput, status *domain.CaseStatus, targetStatus domain.CaseStatus, sig SubmitForApprovalSignal, logger log.Logger) {
	if !isValidTransition(*status, domain.CaseStatusPendingApproval) {
		logger.Warn("submit_for_approval: invalid transition to PENDING_APPROVAL", "from", *status)
		return
	}
	if err := workflow.ExecuteActivity(ctx, a.PersistTransitionActivity, PersistTransitionInput{
		TenantID: in.TenantID, BreakID: in.BreakID, From: *status, To: domain.CaseStatusPendingApproval,
		Actor: sig.Actor, EventType: approvalRequestedEventType,
	}).Get(ctx, nil); err != nil {
		logger.Error("persist pending_approval failed", "error", err)
		return
	}
	*status = domain.CaseStatusPendingApproval

	childCtx := workflow.WithChildOptions(ctx, workflow.ChildWorkflowOptions{
		WorkflowID: ApprovalWorkflowID(in.BreakID),
	})
	var result ApprovalWorkflowResult
	err := workflow.ExecuteChildWorkflow(childCtx, ApprovalWorkflow, ApprovalWorkflowInput{
		BreakID: in.BreakID, TenantID: in.TenantID, MakerUserID: sig.Actor.UserID,
		Action: string(targetStatus), RequiredApprovals: 1,
	}).Get(ctx, &result)
	if err != nil {
		logger.Error("approval child workflow failed", "error", err)
		return
	}

	to := domain.CaseStatusAssigned // rejection returns to work
	eventType := "break.approval_rejected"
	if result.Approved {
		to = targetStatus
		eventType = "break.approval_confirmed"
	}
	if err := workflow.ExecuteActivity(ctx, a.PersistTransitionActivity, PersistTransitionInput{
		TenantID: in.TenantID, BreakID: in.BreakID, From: domain.CaseStatusPendingApproval, To: to,
		Actor: Actor{UserID: "system", Role: "SYSTEM"}, EventType: eventType,
	}).Get(ctx, nil); err != nil {
		logger.Error("persist approval outcome failed", "error", err)
		return
	}
	*status = to
}
