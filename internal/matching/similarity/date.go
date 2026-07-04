package similarity

import "time"

// DateProximity returns 1.0 for zero day-distance between a and b,
// decaying linearly to 0.0 at windowDays, and 0.0 beyond - plain
// calendar-day distance, per plans/task/core/11 Implementation Notes:
// calendar-aware business-day/holiday-calendar support is explicitly not
// required for MVP (deferred, noted as a follow-up).
func DateProximity(a, b time.Time, windowDays int) float64 {
	if windowDays <= 0 {
		if a.Equal(b) || sameCalendarDay(a, b) {
			return 1
		}
		return 0
	}

	days := daysBetween(a, b)
	if days >= float64(windowDays) {
		return 0
	}
	score := 1 - days/float64(windowDays)
	if score < 0 {
		score = 0
	}
	return score
}

func sameCalendarDay(a, b time.Time) bool {
	ay, am, ad := a.Date()
	by, bm, bd := b.Date()
	return ay == by && am == bm && ad == bd
}

func daysBetween(a, b time.Time) float64 {
	diff := a.Sub(b)
	if diff < 0 {
		diff = -diff
	}
	return diff.Hours() / 24
}
