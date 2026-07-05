package notify

import (
	"math/rand/v2"
	"time"
)

// DefaultMaxAttempts is the configurable default before a delivery is
// marked DEAD_LETTERED (plans/task/core/21 Implementation Notes).
const DefaultMaxAttempts = 8

// backoffSchedule is the fixed exponential-backoff-with-jitter table
// plans/task/core/21 Implementation Notes specifies verbatim: "1m, 5m,
// 30m, 2h, 12h; configurable max attempts, default 8." Attempts beyond
// the table's length reuse its last (longest) interval, rather than
// growing unbounded or panicking on an out-of-range index.
var backoffSchedule = []time.Duration{
	1 * time.Minute,
	5 * time.Minute,
	30 * time.Minute,
	2 * time.Hour,
	12 * time.Hour,
}

// NextAttemptDelay returns how long to wait before retrying a delivery
// that has already failed attemptNumber times (1-indexed: the delay
// AFTER the first failure is NextAttemptDelay(1)). Jitter is +/-10% of
// the base interval, avoiding a thundering-herd of retries all landing
// on the exact same instant when many deliveries fail at once (e.g. one
// tenant endpoint going down affects every subscription's every pending
// delivery simultaneously).
func NextAttemptDelay(attemptNumber int) time.Duration {
	idx := attemptNumber - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(backoffSchedule) {
		idx = len(backoffSchedule) - 1
	}
	base := backoffSchedule[idx]
	jitter := time.Duration(float64(base) * 0.10 * (rand.Float64()*2 - 1)) //nolint:gosec // retry jitter, not security-sensitive
	return base + jitter
}
