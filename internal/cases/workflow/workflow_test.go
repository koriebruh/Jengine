package workflow_test

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/testsuite"

	"github.com/koriebruh/Jengine/internal/cases/workflow"
	"github.com/koriebruh/Jengine/internal/domain"
)

// testEnv bundles the environment together with the SAME *Activities
// instance registered on it - env.OnActivity(...) must reference a
// BOUND method value off this exact instance (a.SomeMethod, matching
// how workflow.go itself calls workflow.ExecuteActivity(ctx,
// a.SomeMethod, ...)), not the unbound method expression
// (*Activities).SomeMethod - the two have different function shapes
// (bound erases the receiver as an argument, unbound doesn't), and
// go.temporal.io/sdk/testsuite's mock matching is shape-sensitive.
// Found via this test file's own first run: every OnActivity call
// mismatched with "provided 2 arguments, mocked for 3."
type testEnv struct {
	env *testsuite.TestWorkflowEnvironment
	a   *workflow.Activities
}

func setupEnv(t *testing.T) *testEnv {
	t.Helper()
	suite := &testsuite.WorkflowTestSuite{}
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterWorkflow(workflow.BreakLifecycleWorkflow)
	env.RegisterWorkflow(workflow.ApprovalWorkflow)
	a := &workflow.Activities{}
	env.RegisterActivity(a)
	return &testEnv{env: env, a: a}
}

func baseInput(breakID uuid.UUID) workflow.BreakLifecycleWorkflowInput {
	return workflow.BreakLifecycleWorkflowInput{
		BreakID: breakID, TenantID: uuid.New(), InitialStatus: domain.CaseStatusOpen,
		OpenedAt: time.Now(),
	}
}

func TestBreakLifecycleWorkflow_HappyPath_OpenAssignResolve(t *testing.T) {
	te := setupEnv(t)
	env, a := te.env, te.a
	breakID := uuid.New()
	in := baseInput(breakID)

	env.OnActivity(a.AutoAssignActivity, mock.Anything, mock.Anything).
		Return(workflow.AutoAssignResult{Assignee: "analyst-1"}, nil).Once()
	env.OnActivity(a.ComputeSLAActivity, mock.Anything, mock.Anything).
		Return(workflow.ComputeSLAResult{SLADueAt: in.OpenedAt.Add(24 * time.Hour)}, nil).Once()
	env.OnActivity(a.PersistTransitionActivity, mock.Anything, mock.Anything).
		Return(nil)

	// ASSIGNED -> RESOLVED isn't a valid transition (RESOLVED requires
	// IN_PROGRESS first, per the ported table in state.go) - IN_PROGRESS
	// has no dedicated named signal (plans/docs/05-case-management.md
	// §6.1's own diagram doesn't give it one either), so it goes
	// through the generic SignalTransition.
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(workflow.SignalTransition, workflow.GenericTransitionSignal{
			To: domain.CaseStatusInProgress, Actor: workflow.Actor{UserID: "analyst-1", Role: "Analyst"},
		})
	}, time.Second)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(workflow.SignalResolve, workflow.TransitionSignal{
			Actor: workflow.Actor{UserID: "analyst-1", Role: "Analyst"}, Comment: "fixed",
		})
	}, 2*time.Second)
	env.SetTestTimeout(5 * time.Second)

	env.ExecuteWorkflow(workflow.BreakLifecycleWorkflow, in)

	env.AssertExpectations(t)
}

func TestBreakLifecycleWorkflow_Escalation(t *testing.T) {
	te := setupEnv(t)
	env, a := te.env, te.a
	breakID := uuid.New()
	in := baseInput(breakID)

	env.OnActivity(a.AutoAssignActivity, mock.Anything, mock.Anything).
		Return(workflow.AutoAssignResult{Assignee: "analyst-1"}, nil).Once()
	env.OnActivity(a.ComputeSLAActivity, mock.Anything, mock.Anything).
		Return(workflow.ComputeSLAResult{SLADueAt: in.OpenedAt.Add(24 * time.Hour)}, nil).Once()
	// testsuite auto-fires any still-pending timer (here, the SLA
	// warning/breach timers, since this test's escalate signal doesn't
	// itself consume them) once the environment goes idle at test end -
	// mock this too so that doesn't panic as an unregistered real call.
	env.OnActivity(a.EmitOutboxEventActivity, mock.Anything, mock.Anything).Return(nil).Maybe()

	var transitions []domain.CaseStatus
	env.OnActivity(a.PersistTransitionActivity, mock.Anything, mock.Anything).
		Run(func(args mock.Arguments) {
			in := args.Get(1).(workflow.PersistTransitionInput)
			transitions = append(transitions, in.To)
		}).Return(nil)

	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(workflow.SignalEscalate, workflow.TransitionSignal{
			Actor: workflow.Actor{UserID: "manager-1", Role: "Recon Manager"},
		})
	}, time.Second)
	env.SetTestTimeout(5 * time.Second)

	env.ExecuteWorkflow(workflow.BreakLifecycleWorkflow, in)

	env.AssertExpectations(t)
	require.Contains(t, transitions, domain.CaseStatusEscalated)
}

func TestBreakLifecycleWorkflow_BackfillResume_DoesNotReassign(t *testing.T) {
	te := setupEnv(t)
	env, a := te.env, te.a
	breakID := uuid.New()
	in := workflow.BreakLifecycleWorkflowInput{
		BreakID: breakID, TenantID: uuid.New(), InitialStatus: domain.CaseStatusInProgress,
		OpenedAt: time.Now(), Assignee: "analyst-existing",
		SLADueAt: timePtr(time.Now().Add(24 * time.Hour)),
	}

	// AutoAssignActivity must NEVER be called when resuming a
	// backfilled case that's already past OPEN - plans/task/core/20
	// Common Pitfalls: "a real regression a user would notice
	// immediately." Not registering a .Return() for it means the mock
	// framework fails the test if it's called at all.
	env.OnActivity(a.PersistTransitionActivity, mock.Anything, mock.Anything).Return(nil)
	env.OnActivity(a.EmitOutboxEventActivity, mock.Anything, mock.Anything).Return(nil).Maybe()

	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(workflow.SignalResolve, workflow.TransitionSignal{
			Actor: workflow.Actor{UserID: "analyst-existing", Role: "Analyst"},
		})
	}, time.Second)
	env.SetTestTimeout(5 * time.Second)

	env.ExecuteWorkflow(workflow.BreakLifecycleWorkflow, in)

	env.AssertExpectations(t)
	env.AssertNotCalled(t, "AutoAssignActivity", mock.Anything, mock.Anything)
}

func TestBreakLifecycleWorkflow_SLATimers_WarningAndBreach(t *testing.T) {
	te := setupEnv(t)
	env, a := te.env, te.a
	breakID := uuid.New()
	openedAt := time.Now()
	in := workflow.BreakLifecycleWorkflowInput{
		BreakID: breakID, TenantID: uuid.New(), InitialStatus: domain.CaseStatusOpen, OpenedAt: openedAt,
	}

	env.OnActivity(a.AutoAssignActivity, mock.Anything, mock.Anything).
		Return(workflow.AutoAssignResult{Assignee: "analyst-1"}, nil).Once()
	env.OnActivity(a.ComputeSLAActivity, mock.Anything, mock.Anything).
		Return(workflow.ComputeSLAResult{SLADueAt: openedAt.Add(4 * time.Hour)}, nil).Once()
	env.OnActivity(a.PersistTransitionActivity, mock.Anything, mock.Anything).Return(nil)

	var emittedEvents []string
	env.OnActivity(a.EmitOutboxEventActivity, mock.Anything, mock.Anything).
		Run(func(args mock.Arguments) {
			in := args.Get(1).(workflow.EmitOutboxEventInput)
			emittedEvents = append(emittedEvents, in.EventType)
		}).Return(nil)

	// No real sleep: testsuite's simulated clock advances directly to
	// each NewTimer's fire point (plans/task/core/20's own DoD: "verify
	// SLA timer firing at 75%/100% without real wall-clock sleeps").
	// The breach handler auto-escalates to ESCALATED; send "assign" (a
	// valid ESCALATED -> ASSIGNED transition) shortly after so the test
	// has a clean stopping point - reaching RESOLVED isn't this test's
	// concern, only that both SLA events actually fired.
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(workflow.SignalAssign, workflow.AssignSignal{
			Assignee: "analyst-1", Actor: workflow.Actor{UserID: "analyst-1", Role: "Analyst"},
		})
	}, 4*time.Hour+time.Minute)
	env.SetTestTimeout(10 * time.Second)

	env.ExecuteWorkflow(workflow.BreakLifecycleWorkflow, in)

	env.AssertExpectations(t)
	require.Contains(t, emittedEvents, "break.sla_warning")
	require.Contains(t, emittedEvents, "break.sla_breached")
}

// TestBreakLifecycleWorkflow_RejectThenResubmit is plans/task/core/20's
// own DoD scenario: an approval rejection returns the parent to
// ASSIGNED (not a terminal state), and a second submit_for_approval
// afterward must genuinely start a NEW ApprovalWorkflow child - proving
// the child's deterministic ID (approvalWorkflowID) is reusable once
// the first execution under that ID has closed, not a duplicate-start
// error.
func TestBreakLifecycleWorkflow_RejectThenResubmit(t *testing.T) {
	te := setupEnv(t)
	env, a := te.env, te.a
	breakID := uuid.New()
	in := baseInput(breakID)

	env.OnActivity(a.AutoAssignActivity, mock.Anything, mock.Anything).
		Return(workflow.AutoAssignResult{Assignee: "analyst-1"}, nil).Once()
	env.OnActivity(a.ComputeSLAActivity, mock.Anything, mock.Anything).
		Return(workflow.ComputeSLAResult{SLADueAt: in.OpenedAt.Add(24 * time.Hour)}, nil).Once()
	env.OnActivity(a.EmitOutboxEventActivity, mock.Anything, mock.Anything).Return(nil).Maybe()
	env.OnActivity(a.AuthorizeApprovalActivity, mock.Anything, mock.Anything).
		Return(workflow.AuthorizeApprovalResult{Authorized: true}, nil)

	var transitions []domain.CaseStatus
	env.OnActivity(a.PersistTransitionActivity, mock.Anything, mock.Anything).
		Run(func(args mock.Arguments) {
			pin := args.Get(1).(workflow.PersistTransitionInput)
			transitions = append(transitions, pin.To)
		}).Return(nil)

	// PENDING_APPROVAL is only reachable from IN_PROGRESS (not directly
	// from ASSIGNED) per the ported transition table - IN_PROGRESS has
	// no dedicated signal name (SignalTransition, same as the happy-path
	// test above), and must be re-entered after the reject sends the
	// case back to ASSIGNED before resubmitting.
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(workflow.SignalTransition, workflow.GenericTransitionSignal{
			To: domain.CaseStatusInProgress, Actor: workflow.Actor{UserID: "analyst-1", Role: "Analyst"},
		})
	}, time.Second)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(workflow.SignalSubmitForApproval, workflow.SubmitForApprovalSignal{
			Actor: workflow.Actor{UserID: "analyst-1", Role: "Analyst"}, TargetStatus: domain.CaseStatusResolved,
		})
	}, 2*time.Second)
	// SignalApprove targets the CHILD ApprovalWorkflow, not the parent
	// under test - env.SignalWorkflow only reaches the TOP-LEVEL
	// workflow (BreakLifecycleWorkflow here), so this must use
	// SignalWorkflowByID with the child's own deterministic ID. Found
	// via this test's own first run: env.SignalWorkflow(SignalApprove,
	// ...) silently went nowhere (no channel of that name at the
	// parent), leaving the child - and the test - stuck.
	env.RegisterDelayedCallback(func() {
		require.NoError(t, env.SignalWorkflowByID(workflow.ApprovalWorkflowID(breakID), workflow.SignalApprove, workflow.ApproveRejectSignal{
			ApproverUserID: "approver-1", ApproverRole: "Approver", Approve: false,
		}))
	}, 3*time.Second)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(workflow.SignalTransition, workflow.GenericTransitionSignal{
			To: domain.CaseStatusInProgress, Actor: workflow.Actor{UserID: "analyst-1", Role: "Analyst"},
		})
	}, 4*time.Second)
	// Resubmit - a genuinely NEW child workflow under the same
	// deterministic ID as the first (now-closed) one.
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(workflow.SignalSubmitForApproval, workflow.SubmitForApprovalSignal{
			Actor: workflow.Actor{UserID: "analyst-1", Role: "Analyst"}, TargetStatus: domain.CaseStatusResolved,
		})
	}, 5*time.Second)
	env.RegisterDelayedCallback(func() {
		require.NoError(t, env.SignalWorkflowByID(workflow.ApprovalWorkflowID(breakID), workflow.SignalApprove, workflow.ApproveRejectSignal{
			ApproverUserID: "approver-2", ApproverRole: "Approver", Approve: true,
		}))
	}, 6*time.Second)
	env.SetTestTimeout(10 * time.Second)

	env.ExecuteWorkflow(workflow.BreakLifecycleWorkflow, in)

	env.AssertExpectations(t)
	require.Contains(t, transitions, domain.CaseStatusPendingApproval)
	require.Contains(t, transitions, domain.CaseStatusAssigned)
	require.Contains(t, transitions, domain.CaseStatusResolved)
}

func timePtr(t time.Time) *time.Time { return &t }
