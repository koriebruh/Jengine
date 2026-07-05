package notify_test

import (
	"testing"
	"time"

	"github.com/koriebruh/Jengine/internal/notify"
)

func TestNextAttemptDelay_MatchesDocumentedSchedule(t *testing.T) {
	// plans/task/core/21 Implementation Notes: "1m, 5m, 30m, 2h, 12h" -
	// tolerance covers the +/-10% jitter this package adds.
	tests := []struct {
		attempt  int
		wantBase time.Duration
	}{
		{1, time.Minute},
		{2, 5 * time.Minute},
		{3, 30 * time.Minute},
		{4, 2 * time.Hour},
		{5, 12 * time.Hour},
		{6, 12 * time.Hour}, // beyond the table: reuses the last (longest) interval
		{100, 12 * time.Hour},
	}
	for _, tt := range tests {
		got := notify.NextAttemptDelay(tt.attempt)
		lower := time.Duration(float64(tt.wantBase) * 0.85)
		upper := time.Duration(float64(tt.wantBase) * 1.15)
		if got < lower || got > upper {
			t.Errorf("NextAttemptDelay(%d) = %v, want within [%v, %v] (base %v +/- jitter)", tt.attempt, got, lower, upper, tt.wantBase)
		}
	}
}

func TestNextAttemptDelay_ZeroOrNegativeAttemptUsesFirstInterval(t *testing.T) {
	got := notify.NextAttemptDelay(0)
	if got < 50*time.Second || got > 70*time.Second {
		t.Errorf("NextAttemptDelay(0) = %v, want ~1m", got)
	}
}
