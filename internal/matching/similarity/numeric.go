package similarity

import "github.com/shopspring/decimal"

// NumericCloseness returns 1.0 when a and b are within tolerance
// (absolute and/or percent, resolved against a's magnitude), degrading
// linearly to 0.0 at 2x the tolerance band, and 0.0 beyond - a documented,
// simple degradation curve (plans/task/core/11 Implementation Notes
// explicitly allows "some documented curve," not a specific one).
// Symmetric tolerance only for MVP - asymmetric tolerance bands are
// deferred, not required for MVP sign-off.
func NumericCloseness(a, b decimal.Decimal, absolute decimal.Decimal, percent float64) float64 {
	diff := a.Sub(b).Abs()

	tolerance := absolute
	if percent > 0 {
		pctTolerance := a.Abs().Mul(decimal.NewFromFloat(percent))
		if pctTolerance.GreaterThan(tolerance) {
			tolerance = pctTolerance
		}
	}
	if tolerance.IsZero() {
		if diff.IsZero() {
			return 1
		}
		return 0
	}

	if diff.LessThanOrEqual(tolerance) {
		return 1
	}

	cutoff := tolerance.Mul(decimal.NewFromInt(2))
	if diff.GreaterThanOrEqual(cutoff) {
		return 0
	}

	// Linear degradation between tolerance and 2x tolerance.
	span := cutoff.Sub(tolerance)
	over := diff.Sub(tolerance)
	ratio, _ := over.Div(span).Float64()
	score := 1 - ratio
	if score < 0 {
		score = 0
	}
	return score
}
