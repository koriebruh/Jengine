package similarity_test

import (
	"math"
	"testing"

	"github.com/koriebruh/Jengine/internal/matching/similarity"
)

func almostEqual(a, b, tol float64) bool {
	return math.Abs(a-b) <= tol
}

// Known reference test vectors for Jaro-Winkler (plans/task/core/11
// Common Pitfalls: check against known vectors, not "looks reasonable" -
// these are the textbook values every Jaro-Winkler implementation is
// checked against).
func TestJaroWinkler_KnownVectors(t *testing.T) {
	cases := []struct {
		s1, s2 string
		want   float64
	}{
		{"MARTHA", "MARHTA", 0.961},
		{"DIXON", "DICKSONX", 0.813},
		{"JELLYFISH", "SMELLYFISH", 0.896},
		{"", "", 1.0},
		{"SAME", "SAME", 1.0},
		{"ABC", "XYZ", 0.0},
	}

	for _, c := range cases {
		t.Run(c.s1+"_"+c.s2, func(t *testing.T) {
			got := similarity.JaroWinkler(c.s1, c.s2)
			if !almostEqual(got, c.want, 0.005) {
				t.Errorf("JaroWinkler(%q, %q) = %.4f, want ~%.4f", c.s1, c.s2, got, c.want)
			}
		})
	}
}

func TestJaroSimilarity_PlainJaroNoWinklerBoost(t *testing.T) {
	// Plain Jaro (no prefix boost) for MARTHA/MARHTA is 0.944, distinctly
	// lower than the Winkler-boosted 0.961 - proves the two are actually
	// different (i.e. the Winkler boost is really being applied, not
	// accidentally a no-op or double-counted).
	jaro := similarity.JaroSimilarity("MARTHA", "MARHTA")
	jw := similarity.JaroWinkler("MARTHA", "MARHTA")
	if !almostEqual(jaro, 0.944, 0.005) {
		t.Errorf("JaroSimilarity(MARTHA, MARHTA) = %.4f, want ~0.944", jaro)
	}
	if jw <= jaro {
		t.Errorf("expected Winkler-boosted score (%.4f) to exceed plain Jaro (%.4f)", jw, jaro)
	}
}
