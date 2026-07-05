package workflow

import "github.com/koriebruh/Jengine/internal/domain"

// allowedTransitions is PORTED from internal/cases/state.go verbatim
// (plans/task/core/20 Implementation Notes: "Port task 13's existing
// transition-validity table... into the signal handlers rather than
// inventing a new one - the state diagram in §6.1 must match what task
// 13 already implemented"). Duplicated rather than imported so this
// package doesn't depend on internal/cases (see this package's own doc
// comment on why: internal/cases/temporal_lifecycle.go, the adapter
// implementing cases.LifecycleService, imports THIS package - importing
// back would cycle).
var allowedTransitions = map[domain.CaseStatus][]domain.CaseStatus{
	domain.CaseStatusOpen:            {domain.CaseStatusAssigned},
	domain.CaseStatusAssigned:        {domain.CaseStatusInProgress, domain.CaseStatusEscalated},
	domain.CaseStatusInProgress:      {domain.CaseStatusPendingApproval, domain.CaseStatusResolved, domain.CaseStatusEscalated},
	domain.CaseStatusPendingApproval: {domain.CaseStatusResolved, domain.CaseStatusAssigned, domain.CaseStatusWrittenOff},
	domain.CaseStatusEscalated:       {domain.CaseStatusAssigned},
	domain.CaseStatusResolved:        {domain.CaseStatusReopened},
	domain.CaseStatusWrittenOff:      {domain.CaseStatusReopened},
	domain.CaseStatusReopened:        {domain.CaseStatusAssigned},
}

func isValidTransition(from, to domain.CaseStatus) bool {
	for _, candidate := range allowedTransitions[from] {
		if candidate == to {
			return true
		}
	}
	return false
}

// requiresApprovalTransition mirrors cases.requiresApproval - true only
// for the two transitions OUT of PENDING_APPROVAL that a maker-checker
// gate must guard.
func requiresApprovalTransition(from, to domain.CaseStatus) bool {
	return from == domain.CaseStatusPendingApproval && (to == domain.CaseStatusResolved || to == domain.CaseStatusWrittenOff)
}

// approvalRequestedEventType mirrors cases.approvalRequestedEventType.
const approvalRequestedEventType = "approval.requested"
