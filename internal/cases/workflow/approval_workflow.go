package workflow

import (
	"time"

	"github.com/google/uuid"
	"go.temporal.io/sdk/workflow"
)

// SignalApprove is the signal name internal/cases's
// TemporalLifecycleService.DecideApproval sends directly to a running
// ApprovalWorkflow child (targeting its own deterministic workflow ID,
// approvalWorkflowID(breakID) - see workflow.go) - a separate name space
// from the parent BreakLifecycleWorkflow's own signals.
const SignalApprove = "approve_reject"

// reminderInterval is how often ApprovalWorkflow re-emits a
// case.approval_requested outbox event while pending
// (plans/docs/05-case-management.md §6.4: "automatic reminders").
const reminderInterval = 24 * time.Hour

// ApprovalWorkflowInput starts one maker-checker approval cycle.
type ApprovalWorkflowInput struct {
	BreakID           uuid.UUID
	TenantID          uuid.UUID
	MakerUserID       string
	Action            string // "confirm_low_confidence_match" | "write_off" | "rule_activation" (here: the target CaseStatus name)
	RequiredApprovals int    // configurable multi-level chains, e.g. 2 for write-offs > $1M
}

// ApprovalWorkflowResult is what the parent BreakLifecycleWorkflow
// blocks on.
type ApprovalWorkflowResult struct {
	Approved  bool
	Approvers []string
}

// ApprovalWorkflow waits on repeated approve/reject signals until
// RequiredApprovals distinct approvers have signed, or any single
// reject (plans/docs/05-case-management.md §6.4). Enforces maker !=
// checker by calling AuthorizeApprovalActivity on each incoming approve
// signal - a duplicate signal from an already-recorded approver, or one
// from the maker themselves, is rejected as a no-op, never counted as a
// second valid approval (plans/task/core/20 Implementation Notes).
func ApprovalWorkflow(ctx workflow.Context, in ApprovalWorkflowInput) (ApprovalWorkflowResult, error) {
	ctx = workflow.WithActivityOptions(ctx, defaultActivityOptions)
	logger := workflow.GetLogger(ctx)

	var a *Activities // see BreakLifecycleWorkflow's own comment on this pattern.

	requiredApprovals := in.RequiredApprovals
	if requiredApprovals <= 0 {
		requiredApprovals = 1
	}

	approveCh := workflow.GetSignalChannel(ctx, SignalApprove)
	approvedBy := make(map[string]bool)

	// Reminder timer - re-emits case.approval_requested at
	// reminderInterval while pending. Re-armed each loop iteration
	// (workflow.NewTimer must be called fresh each time it fires, since
	// a Future is one-shot).
	reminderTimer := workflow.NewTimer(ctx, reminderInterval)

	for {
		selector := workflow.NewSelector(ctx)
		done := false
		var result ApprovalWorkflowResult

		selector.AddFuture(reminderTimer, func(f workflow.Future) {
			payload := mustJSON(map[string]any{"break_id": in.BreakID, "action": in.Action})
			if err := workflow.ExecuteActivity(ctx, a.EmitOutboxEventActivity, EmitOutboxEventInput{
				TenantID: in.TenantID, AggregateID: in.BreakID, EventType: "case.approval_requested",
				Topic: "case.events.default", Payload: payload,
			}).Get(ctx, nil); err != nil {
				logger.Error("emit approval reminder outbox event failed", "error", err)
			}
			reminderTimer = workflow.NewTimer(ctx, reminderInterval)
		})

		selector.AddReceive(approveCh, func(c workflow.ReceiveChannel, more bool) {
			var sig ApproveRejectSignal
			c.Receive(ctx, &sig)

			if !sig.Approve {
				result = ApprovalWorkflowResult{Approved: false}
				done = true
				return
			}

			if approvedBy[sig.ApproverUserID] {
				logger.Warn("duplicate approval signal from same approver, ignoring", "approver", sig.ApproverUserID)
				return
			}

			var authResult AuthorizeApprovalResult
			if err := workflow.ExecuteActivity(ctx, a.AuthorizeApprovalActivity, AuthorizeApprovalInput{
				TenantID: in.TenantID, MakerUserID: in.MakerUserID,
				ApproverUserID: sig.ApproverUserID, ApproverRole: sig.ApproverRole,
			}).Get(ctx, &authResult); err != nil {
				logger.Error("authorize approval activity failed", "error", err)
				return
			}
			if !authResult.Authorized {
				logger.Warn("approval rejected by authorization check", "approver", sig.ApproverUserID, "reason", authResult.Reason)
				return
			}

			approvedBy[sig.ApproverUserID] = true
			if len(approvedBy) >= requiredApprovals {
				approvers := make([]string, 0, len(approvedBy))
				for u := range approvedBy {
					approvers = append(approvers, u)
				}
				result = ApprovalWorkflowResult{Approved: true, Approvers: approvers}
				done = true
			}
		})

		selector.Select(ctx)
		if done {
			return result, nil
		}
	}
}
