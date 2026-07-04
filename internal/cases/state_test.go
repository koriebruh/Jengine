package cases_test

import (
	"testing"

	"github.com/koriebruh/Jengine/internal/cases"
)

func TestIsValidTransition_AllowedTransitionsSucceed(t *testing.T) {
	cases_ := []struct{ from, to cases.BreakStatus }{
		{cases.BreakOpen, cases.BreakAssigned},
		{cases.BreakAssigned, cases.BreakInProgress},
		{cases.BreakAssigned, cases.BreakEscalated},
		{cases.BreakInProgress, cases.BreakPendingApproval},
		{cases.BreakInProgress, cases.BreakResolved},
		{cases.BreakInProgress, cases.BreakEscalated},
		{cases.BreakPendingApproval, cases.BreakResolved},
		{cases.BreakPendingApproval, cases.BreakAssigned},
		{cases.BreakPendingApproval, cases.BreakWrittenOff},
		{cases.BreakEscalated, cases.BreakAssigned},
		{cases.BreakResolved, cases.BreakReopened},
		{cases.BreakWrittenOff, cases.BreakReopened},
		{cases.BreakReopened, cases.BreakAssigned},
	}
	for _, c := range cases_ {
		if !cases.IsValidTransition(c.from, c.to) {
			t.Errorf("expected %s -> %s to be a valid transition", c.from, c.to)
		}
	}
}

func TestIsValidTransition_DisallowedTransitionsRejected(t *testing.T) {
	cases_ := []struct{ from, to cases.BreakStatus }{
		{cases.BreakOpen, cases.BreakInProgress},            // must go through ASSIGNED first
		{cases.BreakOpen, cases.BreakWrittenOff},            // WRITTEN_OFF requires PENDING_APPROVAL first
		{cases.BreakAssigned, cases.BreakWrittenOff},        // same
		{cases.BreakInProgress, cases.BreakWrittenOff},      // same - only reachable from PENDING_APPROVAL
		{cases.BreakEscalated, cases.BreakWrittenOff},       // same
		{cases.BreakResolved, cases.BreakAssigned},          // terminal except REOPENED
		{cases.BreakWrittenOff, cases.BreakAssigned},        // terminal except REOPENED
		{cases.BreakOpen, cases.BreakOpen},                  // identity is not a transition
		{cases.BreakPendingApproval, cases.BreakInProgress}, // not listed
		{cases.BreakReopened, cases.BreakResolved},          // REOPENED only goes to ASSIGNED
	}
	for _, c := range cases_ {
		if cases.IsValidTransition(c.from, c.to) {
			t.Errorf("expected %s -> %s to be rejected as an invalid transition", c.from, c.to)
		}
	}
}
