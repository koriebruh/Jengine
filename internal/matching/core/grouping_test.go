package core_test

import (
	"testing"
	"testing/quick"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"

	"github.com/koriebruh/Jengine/internal/matching/core"
)

func recordWithAmount(amount string) core.MatchableRecord {
	return core.MatchableRecord{ID: uuid.New(), BaseAmount: decimal.RequireFromString(amount)}
}

func TestFindGroup_FindsExactSumSubset(t *testing.T) {
	candidates := []core.MatchableRecord{
		recordWithAmount("30.00"),
		recordWithAmount("70.00"),
		recordWithAmount("15.00"),
	}
	target := decimal.RequireFromString("100.00")
	tol := core.ToleranceSpec{Kind: "exact"}

	group, found := core.FindGroup(candidates, target, tol, 5)
	if !found {
		t.Fatal("expected a group summing to 100.00 to be found")
	}
	sum := decimal.Zero
	for _, r := range group {
		sum = sum.Add(r.BaseAmount)
	}
	if !sum.Equal(target) {
		t.Errorf("expected group sum %s, got %s", target, sum)
	}
}

func TestFindGroup_WithinNumericTolerance(t *testing.T) {
	candidates := []core.MatchableRecord{
		recordWithAmount("49.99"),
		recordWithAmount("50.02"),
	}
	target := decimal.RequireFromString("100.00")
	tol := core.ToleranceSpec{Kind: "numeric", Absolute: decimal.RequireFromString("0.05")}

	group, found := core.FindGroup(candidates, target, tol, 5)
	if !found {
		t.Fatal("expected a group within 0.05 tolerance of 100.00 to be found")
	}
	if len(group) != 2 {
		t.Fatalf("expected both records in the group, got %d", len(group))
	}
}

func TestFindGroup_NoGroupWithinTolerance(t *testing.T) {
	candidates := []core.MatchableRecord{
		recordWithAmount("10.00"),
		recordWithAmount("20.00"),
	}
	target := decimal.RequireFromString("100.00")
	tol := core.ToleranceSpec{Kind: "exact"}

	_, found := core.FindGroup(candidates, target, tol, 5)
	if found {
		t.Fatal("expected no group to be found - no subset sums to 100.00")
	}
}

// TestFindGroup_NeverExceedsMaxGroupSize is a property-style test: across
// many synthetic candidate sets, the returned group (when found) never
// exceeds maxGroupSize (plans/task/core/10 DoD).
func TestFindGroup_NeverExceedsMaxGroupSize(t *testing.T) {
	const maxGroupSize = 3
	// 6 candidates of 1.00 each; target 10.00 is unreachable by any
	// subset of size <= 3 (max sum = 3.00), so this also exercises the
	// "search always terminates without finding anything" path across a
	// non-trivial combination space (C(6,1)+C(6,2)+C(6,3) = 6+15+20 = 41
	// combinations checked).
	candidates := make([]core.MatchableRecord, 6)
	for i := range candidates {
		candidates[i] = recordWithAmount("1.00")
	}

	group, found := core.FindGroup(candidates, decimal.RequireFromString("10.00"), core.ToleranceSpec{Kind: "exact"}, maxGroupSize)
	if found {
		t.Fatalf("expected no group (target unreachable within maxGroupSize), got %+v", group)
	}

	// Now make it reachable at exactly size 3 (sum of 3 x 1.00 = 3.00).
	group, found = core.FindGroup(candidates, decimal.RequireFromString("3.00"), core.ToleranceSpec{Kind: "exact"}, maxGroupSize)
	if !found {
		t.Fatal("expected a group of size 3 summing to 3.00")
	}
	if len(group) > maxGroupSize {
		t.Fatalf("group size %d exceeds maxGroupSize %d", len(group), maxGroupSize)
	}
}

// TestFindGroup_NoDoubleAllocation proves a single FindGroup call never
// returns the same candidate twice within its own result (double-
// allocation within a returned group would silently corrupt an
// aggregation).
func TestFindGroup_NoDoubleAllocation(t *testing.T) {
	candidates := []core.MatchableRecord{
		recordWithAmount("25.00"),
		recordWithAmount("25.00"),
		recordWithAmount("50.00"),
	}
	target := decimal.RequireFromString("100.00")
	tol := core.ToleranceSpec{Kind: "exact"}

	group, found := core.FindGroup(candidates, target, tol, 5)
	if !found {
		t.Fatal("expected a group to be found")
	}
	seen := make(map[uuid.UUID]bool)
	for _, r := range group {
		if seen[r.ID] {
			t.Fatalf("record %s allocated more than once in the same group", r.ID)
		}
		seen[r.ID] = true
	}
}

func TestFindGroup_EmptyCandidatesNeverFound(t *testing.T) {
	_, found := core.FindGroup(nil, decimal.RequireFromString("100.00"), core.ToleranceSpec{Kind: "exact"}, 5)
	if found {
		t.Fatal("expected no group from an empty candidate set")
	}
}

// TestFindGroup_PropertyInvariants is plans/task/core/17's property-based
// test requirement for task 10's bounded one-to-many grouping helper
// (Design Reference: "a testing/quick-style or a small custom generator
// for synthetic candidate sets" - not testing the full many-to-many DP
// solver, which doesn't exist at MVP). Runs the invariants every prior
// hand-picked test in this file checks individually, across hundreds of
// randomly generated candidate sets: whenever FindGroup reports a group,
// that group (1) never exceeds maxGroupSize, (2) contains no candidate
// twice, (3) sums to exactly target (exact-tolerance case), and (4) is a
// genuine subset of the input candidates, not a fabricated result.
func TestFindGroup_PropertyInvariants(t *testing.T) {
	property := func(amountsCents []int16, targetCents int16, maxGroupSizeSeed uint8) bool {
		if len(amountsCents) == 0 || len(amountsCents) > 12 {
			return true // quick.Check will still explore plenty of in-range cases
		}
		maxGroupSize := int(maxGroupSizeSeed%5) + 1 // bound to 1..5

		candidates := make([]core.MatchableRecord, len(amountsCents))
		for i, cents := range amountsCents {
			candidates[i] = core.MatchableRecord{ID: uuid.New(), BaseAmount: decimal.New(int64(cents), -2)}
		}
		target := decimal.New(int64(targetCents), -2)
		tol := core.ToleranceSpec{Kind: "exact"}

		group, found := core.FindGroup(candidates, target, tol, maxGroupSize)
		if !found {
			return true // nothing further to check when no group is claimed
		}

		if len(group) > maxGroupSize {
			return false
		}

		original := make(map[uuid.UUID]bool, len(candidates))
		for _, c := range candidates {
			original[c.ID] = true
		}

		seen := make(map[uuid.UUID]bool, len(group))
		sum := decimal.Zero
		for _, r := range group {
			if seen[r.ID] {
				return false // double allocation within the same group
			}
			seen[r.ID] = true
			if !original[r.ID] {
				return false // fabricated result not present in the input
			}
			sum = sum.Add(r.BaseAmount)
		}

		return sum.Equal(target)
	}

	if err := quick.Check(property, &quick.Config{MaxCount: 500}); err != nil {
		t.Error(err)
	}
}
