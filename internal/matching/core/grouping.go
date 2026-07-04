package core

import "github.com/shopspring/decimal"

// MaxGroupSizeCap is the MVP hard ceiling on CompiledRule.MaxGroupSize,
// documented in exactly one place so plans/task/core/11's DSL compiler
// (which clamps a tenant-authored aggregation_rules.max_group_size
// against it) and this package's own FindGroup default stay consistent -
// plans/task/core/10 Implementation Notes calls for "MVP default small,
// e.g. 5"; this is that number, named so it's never silently redefined
// differently in two places.
const MaxGroupSizeCap = 5

// FindGroup searches candidates (already blocking-bucket-filtered, so
// small) for a subset of size <= maxGroupSize whose summed BaseAmount
// falls within tolerance of targetAmount - the bounded "simple one-to-
// many" MVP helper (plans/docs/11-scalability-roadmap.md §12.2 Phase 0),
// explicitly not the full many-to-many subset-sum/knapsack DP solver
// plans/docs/04-matching-engine.md §5.2 describes (plans/task/core/10
// Non-Goals - that solver and MANY_TO_MANY cardinality generally are
// deferred, not currently assigned a task number in 10-17).
//
// Invariants (see grouping_test.go's property tests): returns at most
// one group, so a caller never double-allocates a candidate across
// multiple groups from a single call; the returned group never exceeds
// maxGroupSize; the search always terminates, bounded by
// C(len(candidates), maxGroupSize) - both inputs are small (candidate
// sets from blocking are already small, this is not a general solver
// over an entire partition), so this stays a bounded brute-force search,
// not combinatorial blowup.
func FindGroup(candidates []MatchableRecord, targetAmount decimal.Decimal, tolerance ToleranceSpec, maxGroupSize int) ([]MatchableRecord, bool) {
	if maxGroupSize <= 0 {
		maxGroupSize = MaxGroupSizeCap
	}
	if maxGroupSize > len(candidates) {
		maxGroupSize = len(candidates)
	}

	indices := make([]int, len(candidates))
	for i := range indices {
		indices[i] = i
	}

	var resultIdx []int
	found := false

	for size := 1; size <= maxGroupSize && !found; size++ {
		combinations(indices, size, func(combo []int) bool {
			sum := decimal.Zero
			for _, idx := range combo {
				sum = sum.Add(candidates[idx].BaseAmount)
			}
			if amountWithinTolerance(sum, targetAmount, tolerance) {
				resultIdx = append([]int{}, combo...)
				found = true
				return false // stop enumerating - first match within this size wins
			}
			return true
		})
	}

	if !found {
		return nil, false
	}
	group := make([]MatchableRecord, len(resultIdx))
	for i, idx := range resultIdx {
		group[i] = candidates[idx]
	}
	return group, true
}

func amountWithinTolerance(sum, target decimal.Decimal, tol ToleranceSpec) bool {
	diff := sum.Sub(target).Abs()
	switch tol.Kind {
	case "numeric":
		if !tol.Absolute.IsZero() {
			return diff.LessThanOrEqual(tol.Absolute)
		}
		if tol.Percent > 0 {
			allowed := target.Abs().Mul(decimal.NewFromFloat(tol.Percent))
			return diff.LessThanOrEqual(allowed)
		}
		return diff.IsZero()
	default: // "exact"
		return diff.IsZero()
	}
}

// combinations calls yield with every size-length combination of
// indices, in increasing-index order, stopping early if yield returns
// false. A standard bounded recursive combination generator - terminates
// because both size and len(indices) are finite and small (callers cap
// them before calling this).
func combinations(indices []int, size int, yield func([]int) bool) bool {
	if size == 0 {
		return yield(nil)
	}

	var helper func(start int, combo []int) bool
	helper = func(start int, combo []int) bool {
		if len(combo) == size {
			return yield(combo)
		}
		for i := start; i < len(indices); i++ {
			if !helper(i+1, append(combo, indices[i])) {
				return false
			}
		}
		return true
	}
	return helper(0, nil)
}
