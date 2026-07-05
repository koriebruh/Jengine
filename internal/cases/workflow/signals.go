package workflow

import "github.com/koriebruh/Jengine/internal/domain"

// AssignSignal is SignalAssign's payload.
type AssignSignal struct {
	Assignee string
	Actor    Actor
}

// CommentSignal is SignalComment's payload.
type CommentSignal struct {
	Actor Actor
	Body  string
}

// TransitionSignal is the payload for the plain (non-approval-gated)
// transition signals: escalate, resolve, reopen.
type TransitionSignal struct {
	Actor   Actor
	Comment string
}

// GenericTransitionSignal is SignalTransition's payload - the catch-all
// for any valid transition not covered by a fixed-name signal (see
// SignalTransition's own doc comment for why this exists: ASSIGNED ->
// IN_PROGRESS has no dedicated action name).
type GenericTransitionSignal struct {
	To      domain.CaseStatus
	Actor   Actor
	Comment string
}

// SubmitForApprovalSignal is the payload for submit_for_approval and
// write_off - both start an ApprovalWorkflow child (plans/task/core/20
// Implementation Notes).
type SubmitForApprovalSignal struct {
	Actor Actor
	// TargetStatus is the status to move to once approved - RESOLVED
	// for a plain submit_for_approval, WRITTEN_OFF for the write_off
	// signal (whose handler hardcodes this rather than reading it from
	// the signal payload, but the field is here for
	// forward-compatibility/testability).
	TargetStatus domain.CaseStatus
}

// ApproveRejectSignal is ApprovalWorkflow's own signal payload (see
// approval_workflow.go) - a separate signal name space from the parent
// workflow's, since it targets the CHILD workflow's deterministic ID
// directly (internal/cases's TemporalLifecycleService.DecideApproval
// signals this child, not the parent).
type ApproveRejectSignal struct {
	ApproverUserID string
	ApproverRole   string
	Approve        bool
	Comment        string
}
