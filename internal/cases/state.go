package cases

import "github.com/koriebruh/Jengine/internal/domain"

// BreakStatus is a type alias for domain.CaseStatus (task 05 already
// defined the full status enum - including REOPENED, added during
// cross-task reconciliation - and domain.Case.Status uses it directly).
// Task 13's own spec sketches a standalone `type BreakStatus string`, but
// since task 05 already exists with an identical value set, aliasing
// avoids a second parallel enum that would need constant conversion at
// every domain.Case/CaseRepository call site.
type BreakStatus = domain.CaseStatus

const (
	BreakOpen            = domain.CaseStatusOpen
	BreakAssigned        = domain.CaseStatusAssigned
	BreakInProgress      = domain.CaseStatusInProgress
	BreakPendingApproval = domain.CaseStatusPendingApproval
	BreakResolved        = domain.CaseStatusResolved
	BreakWrittenOff      = domain.CaseStatusWrittenOff
	BreakEscalated       = domain.CaseStatusEscalated
	BreakReopened        = domain.CaseStatusReopened
)

// allowedTransitions encodes plans/docs/05-case-management.md §6.1's
// lifecycle diagram directly. WRITTEN_OFF only appears as a target from
// PENDING_APPROVAL - "any non-terminal state can be written off" is
// enforced by requiring the transition go through PENDING_APPROVAL
// first (plans/task/core/13 Implementation Notes), not by listing
// WRITTEN_OFF under every other state. Transition() (lifecycle.go)
// additionally enforces maker != checker whenever consuming a
// PENDING_APPROVAL state (moving to RESOLVED or WRITTEN_OFF) - not just
// inside DecideApproval - so a caller can't bypass the maker-checker gate
// by calling Transition directly instead of DecideApproval.
var allowedTransitions = map[BreakStatus][]BreakStatus{
	BreakOpen:            {BreakAssigned},
	BreakAssigned:        {BreakInProgress, BreakEscalated},
	BreakInProgress:      {BreakPendingApproval, BreakResolved, BreakEscalated},
	BreakPendingApproval: {BreakResolved, BreakAssigned, BreakWrittenOff},
	BreakEscalated:       {BreakAssigned},
	BreakResolved:        {BreakReopened},
	BreakWrittenOff:      {BreakReopened},
	BreakReopened:        {BreakAssigned},
}

// IsValidTransition reports whether moving a Break from status "from" to
// status "to" is allowed. Identity transitions are not considered valid.
func IsValidTransition(from, to BreakStatus) bool {
	for _, candidate := range allowedTransitions[from] {
		if candidate == to {
			return true
		}
	}
	return false
}

// requiresApproval reports whether reaching "to" from "from" must have
// passed through PENDING_APPROVAL - used by Transition to enforce the
// maker != checker gate exactly where §6.4 requires it (write-offs and
// approval-consumed resolutions), without requiring every caller to
// remember to call DecideApproval specifically.
func requiresApproval(from, to BreakStatus) bool {
	return from == BreakPendingApproval && (to == BreakResolved || to == BreakWrittenOff)
}
