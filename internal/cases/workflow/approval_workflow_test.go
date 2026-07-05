package workflow_test

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/koriebruh/Jengine/internal/cases/workflow"
)

func TestApprovalWorkflow_SingleApproval(t *testing.T) {
	te := setupEnv(t)
	env, a := te.env, te.a

	env.OnActivity(a.AuthorizeApprovalActivity, mock.Anything, mock.Anything).
		Return(workflow.AuthorizeApprovalResult{Authorized: true}, nil).Once()
	env.OnActivity(a.EmitOutboxEventActivity, mock.Anything, mock.Anything).Return(nil).Maybe()

	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(workflow.SignalApprove, workflow.ApproveRejectSignal{
			ApproverUserID: "approver-1", ApproverRole: "Approver", Approve: true,
		})
	}, time.Second)

	env.ExecuteWorkflow(workflow.ApprovalWorkflow, workflow.ApprovalWorkflowInput{
		BreakID: uuid.New(), TenantID: uuid.New(), MakerUserID: "maker-1",
		Action: "RESOLVED", RequiredApprovals: 1,
	})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	var result workflow.ApprovalWorkflowResult
	require.NoError(t, env.GetWorkflowResult(&result))
	require.True(t, result.Approved)
	require.Equal(t, []string{"approver-1"}, result.Approvers)
	env.AssertExpectations(t)
}

func TestApprovalWorkflow_Reject(t *testing.T) {
	te := setupEnv(t)
	env := te.env

	env.OnActivity(te.a.EmitOutboxEventActivity, mock.Anything, mock.Anything).Return(nil).Maybe()

	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(workflow.SignalApprove, workflow.ApproveRejectSignal{
			ApproverUserID: "approver-1", ApproverRole: "Approver", Approve: false,
		})
	}, time.Second)

	env.ExecuteWorkflow(workflow.ApprovalWorkflow, workflow.ApprovalWorkflowInput{
		BreakID: uuid.New(), TenantID: uuid.New(), MakerUserID: "maker-1",
		Action: "RESOLVED", RequiredApprovals: 1,
	})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	var result workflow.ApprovalWorkflowResult
	require.NoError(t, env.GetWorkflowResult(&result))
	require.False(t, result.Approved)
	env.AssertExpectations(t)
}

func TestApprovalWorkflow_MultiLevel_RequiresDistinctApprovers(t *testing.T) {
	te := setupEnv(t)
	env, a := te.env, te.a

	env.OnActivity(a.AuthorizeApprovalActivity, mock.Anything, mock.Anything).
		Return(workflow.AuthorizeApprovalResult{Authorized: true}, nil)
	env.OnActivity(a.EmitOutboxEventActivity, mock.Anything, mock.Anything).Return(nil).Maybe()

	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(workflow.SignalApprove, workflow.ApproveRejectSignal{
			ApproverUserID: "approver-1", ApproverRole: "Approver", Approve: true,
		})
	}, time.Second)
	// Duplicate signal from the SAME approver - must be ignored, not
	// counted as a second distinct approval (plans/task/core/20
	// Implementation Notes: "rejecting a second signal from the same
	// user as a duplicate/no-op, not a second valid approval").
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(workflow.SignalApprove, workflow.ApproveRejectSignal{
			ApproverUserID: "approver-1", ApproverRole: "Approver", Approve: true,
		})
	}, 2*time.Second)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(workflow.SignalApprove, workflow.ApproveRejectSignal{
			ApproverUserID: "approver-2", ApproverRole: "Recon Manager", Approve: true,
		})
	}, 3*time.Second)

	env.ExecuteWorkflow(workflow.ApprovalWorkflow, workflow.ApprovalWorkflowInput{
		BreakID: uuid.New(), TenantID: uuid.New(), MakerUserID: "maker-1",
		Action: "WRITTEN_OFF", RequiredApprovals: 2,
	})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	var result workflow.ApprovalWorkflowResult
	require.NoError(t, env.GetWorkflowResult(&result))
	require.True(t, result.Approved)
	require.ElementsMatch(t, []string{"approver-1", "approver-2"}, result.Approvers)
}

func TestApprovalWorkflow_MakerCannotApproveOwnRequest(t *testing.T) {
	te := setupEnv(t)
	env, a := te.env, te.a

	// AuthorizeApprovalActivity itself enforces maker != checker - the
	// real activity would return Authorized: false for this case, but
	// this test mocks the activity to isolate ApprovalWorkflow's OWN
	// logic (it must respect the activity's verdict, not just check
	// user-ID equality inline itself).
	env.OnActivity(a.AuthorizeApprovalActivity, mock.Anything, mock.Anything).
		Return(workflow.AuthorizeApprovalResult{Authorized: false, Reason: "maker != checker violation"}, nil).Once()
	env.OnActivity(a.EmitOutboxEventActivity, mock.Anything, mock.Anything).Return(nil).Maybe()

	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(workflow.SignalApprove, workflow.ApproveRejectSignal{
			ApproverUserID: "maker-1", ApproverRole: "Approver", Approve: true,
		})
	}, time.Second)
	// Workflow must still be waiting (not completed) since the only
	// approval attempt was rejected by AuthorizeApprovalActivity - send
	// a real reject to give the test a stopping point.
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(workflow.SignalApprove, workflow.ApproveRejectSignal{
			ApproverUserID: "someone-else", ApproverRole: "Approver", Approve: false,
		})
	}, 2*time.Second)

	env.ExecuteWorkflow(workflow.ApprovalWorkflow, workflow.ApprovalWorkflowInput{
		BreakID: uuid.New(), TenantID: uuid.New(), MakerUserID: "maker-1",
		Action: "RESOLVED", RequiredApprovals: 1,
	})

	require.True(t, env.IsWorkflowCompleted())
	var result workflow.ApprovalWorkflowResult
	require.NoError(t, env.GetWorkflowResult(&result))
	require.False(t, result.Approved)
	env.AssertExpectations(t)
}
