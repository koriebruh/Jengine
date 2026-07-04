package similarity_test

import (
	"testing"
	"time"

	"github.com/shopspring/decimal"

	"github.com/koriebruh/Jengine/internal/matching/similarity"
)

func TestNumericCloseness(t *testing.T) {
	tol := decimal.RequireFromString("0.01")

	t.Run("within tolerance scores 1.0", func(t *testing.T) {
		got := similarity.NumericCloseness(decimal.RequireFromString("100.00"), decimal.RequireFromString("100.01"), tol, 0)
		if got != 1.0 {
			t.Errorf("expected 1.0, got %v", got)
		}
	})

	t.Run("exactly at tolerance boundary scores 1.0", func(t *testing.T) {
		got := similarity.NumericCloseness(decimal.RequireFromString("100.00"), decimal.RequireFromString("100.01"), tol, 0)
		if got != 1.0 {
			t.Errorf("expected 1.0 at the exact boundary, got %v", got)
		}
	})

	t.Run("beyond 2x tolerance scores 0.0", func(t *testing.T) {
		got := similarity.NumericCloseness(decimal.RequireFromString("100.00"), decimal.RequireFromString("100.05"), tol, 0)
		if got != 0.0 {
			t.Errorf("expected 0.0 beyond 2x tolerance, got %v", got)
		}
	})

	t.Run("midway between tolerance and 2x tolerance scores ~0.5", func(t *testing.T) {
		// tolerance=0.01, 2x=0.02, midpoint diff=0.015
		got := similarity.NumericCloseness(decimal.RequireFromString("100.00"), decimal.RequireFromString("100.015"), tol, 0)
		if got < 0.4 || got > 0.6 {
			t.Errorf("expected ~0.5 midway through the degradation band, got %v", got)
		}
	})

	t.Run("percent tolerance", func(t *testing.T) {
		got := similarity.NumericCloseness(decimal.RequireFromString("1000.00"), decimal.RequireFromString("1005.00"), decimal.Zero, 0.01)
		if got != 1.0 {
			t.Errorf("expected 1.0 within 1%% tolerance of 1000, got %v", got)
		}
	})

	t.Run("exact zero-tolerance requires exact equality", func(t *testing.T) {
		got := similarity.NumericCloseness(decimal.RequireFromString("100.00"), decimal.RequireFromString("100.00"), decimal.Zero, 0)
		if got != 1.0 {
			t.Errorf("expected 1.0 for exact equality with zero tolerance, got %v", got)
		}
		got2 := similarity.NumericCloseness(decimal.RequireFromString("100.00"), decimal.RequireFromString("100.01"), decimal.Zero, 0)
		if got2 != 0.0 {
			t.Errorf("expected 0.0 for any difference with zero tolerance, got %v", got2)
		}
	})
}

func TestDateProximity(t *testing.T) {
	base := time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC)

	t.Run("zero distance scores 1.0", func(t *testing.T) {
		if got := similarity.DateProximity(base, base, 7); got != 1.0 {
			t.Errorf("expected 1.0 for identical dates, got %v", got)
		}
	})

	t.Run("at window edge scores 0.0", func(t *testing.T) {
		got := similarity.DateProximity(base, base.AddDate(0, 0, 7), 7)
		if got != 0.0 {
			t.Errorf("expected 0.0 at the window edge, got %v", got)
		}
	})

	t.Run("beyond window scores 0.0", func(t *testing.T) {
		got := similarity.DateProximity(base, base.AddDate(0, 0, 10), 7)
		if got != 0.0 {
			t.Errorf("expected 0.0 beyond the window, got %v", got)
		}
	})

	t.Run("midway through window scores ~0.5", func(t *testing.T) {
		got := similarity.DateProximity(base, base.AddDate(0, 0, 3), 6)
		if got < 0.4 || got > 0.6 {
			t.Errorf("expected ~0.5 midway through a 6-day window, got %v", got)
		}
	})
}

func TestLevenshteinNormalized(t *testing.T) {
	cases := []struct {
		a, b string
		want float64
	}{
		{"kitten", "sitting", 1 - 3.0/7}, // classic edit-distance-3 example
		{"same", "same", 1.0},
		{"", "", 1.0},
		{"abc", "abc", 1.0},
	}
	for _, c := range cases {
		got := similarity.LevenshteinNormalized(c.a, c.b)
		if !almostEqual(got, c.want, 0.001) {
			t.Errorf("LevenshteinNormalized(%q, %q) = %.4f, want %.4f", c.a, c.b, got, c.want)
		}
	}
}
