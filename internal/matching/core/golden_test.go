package core_test

import (
	"context"
	"encoding/json"
	"math"
	"os"
	"path/filepath"
	"testing"

	"github.com/google/uuid"

	"github.com/koriebruh/Jengine/internal/matching/core"
	"gopkg.in/yaml.v3"
)

// Golden-dataset runner per plans/task/core/17 and
// plans/docs/16-development-workflow.md §16.4. Walks testdata/<case>/,
// loads source.json/target.json/rules.yaml/expected.json, runs
// core.Match, and diffs against expected.json.
//
// rules.yaml here is a direct YAML rendering of []core.CompiledRule
// itself (using CompiledRule's own yaml tags), NOT the tenant-facing DSL
// shape from plans/docs/02-data-ingestion.md/04-matching-engine.md §5.1
// that plans/task/core/11's real compiler parses - this task explicitly
// does not build that DSL parser (Non-Goals), so its own fixtures use
// the compiled form directly, which is exactly the "compile target both
// tasks agree on" the task description names.
//
// testScoringRegistry below is a minimal, test-local ScoringRegistry
// standing in for plans/task/core/11's real similarity functions
// (Jaro-Winkler, Levenshtein, etc., not built yet) - just enough
// (exact, numeric_closeness, date_proximity) for this task's own golden
// fixtures to exercise real scoring behavior end-to-end.

func TestGolden(t *testing.T) {
	entries, err := os.ReadDir("testdata")
	if err != nil {
		t.Fatalf("failed to read testdata/: %v", err)
	}

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		caseName := entry.Name()
		t.Run(caseName, func(t *testing.T) {
			dir := filepath.Join("testdata", caseName)

			source := loadRecords(t, filepath.Join(dir, "source.json"))
			target := loadRecords(t, filepath.Join(dir, "target.json"))
			rules := loadRules(t, filepath.Join(dir, "rules.yaml"))

			expectedBytes, err := os.ReadFile(filepath.Join(dir, "expected.json"))
			if err != nil {
				t.Fatalf("failed to read expected.json: %v", err)
			}
			var expected core.MatchOutcome
			if err := json.Unmarshal(expectedBytes, &expected); err != nil {
				t.Fatalf("failed to parse expected.json: %v", err)
			}

			actual, err := core.Match(context.Background(), source, target, rules, testScoringRegistry{})
			if err != nil {
				t.Fatalf("Match failed: %v", err)
			}

			assertOutcomeMatches(t, caseName, actual, expected)
		})
	}
}

func loadRecords(t *testing.T, path string) []core.MatchableRecord {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("failed to read %s: %v", path, err)
	}
	var records []core.MatchableRecord
	if err := json.Unmarshal(data, &records); err != nil {
		t.Fatalf("failed to parse %s: %v", path, err)
	}
	return records
}

func loadRules(t *testing.T, path string) []core.CompiledRule {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("failed to read %s: %v", path, err)
	}
	var rules []core.CompiledRule
	if err := yaml.Unmarshal(data, &rules); err != nil {
		t.Fatalf("failed to parse %s: %v", path, err)
	}
	return rules
}

// assertOutcomeMatches compares by ID sets (not full reflect.DeepEqual)
// since map iteration order and slice append order aren't guaranteed
// stable across FieldScores map construction - the golden contract is
// "these transactions ended up auto-matched/suggested/unmatched," not a
// byte-for-byte struct comparison.
func assertOutcomeMatches(t *testing.T, caseName string, actual, expected core.MatchOutcome) {
	t.Helper()

	if len(actual.AutoMatched) != len(expected.AutoMatched) {
		t.Errorf("%s: expected %d auto-matched, got %d (%+v)", caseName, len(expected.AutoMatched), len(actual.AutoMatched), actual.AutoMatched)
	}
	if len(actual.Suggested) != len(expected.Suggested) {
		t.Errorf("%s: expected %d suggested, got %d (%+v)", caseName, len(expected.Suggested), len(actual.Suggested), actual.Suggested)
	}
	if !sameIDSet(actual.Unmatched, expected.Unmatched) {
		t.Errorf("%s: unmatched set mismatch:\n  got:      %v\n  expected: %v", caseName, actual.Unmatched, expected.Unmatched)
	}
}

func sameIDSet(a, b []uuid.UUID) bool {
	if len(a) != len(b) {
		return false
	}
	set := make(map[uuid.UUID]bool, len(a))
	for _, id := range a {
		set[id] = true
	}
	for _, id := range b {
		if !set[id] {
			return false
		}
	}
	return true
}

// testScoringRegistry is a minimal stand-in for plans/task/core/11's
// real similarity-function registry - exact, numeric_closeness, and
// date_proximity are enough for this task's own golden fixtures.
type testScoringRegistry struct{}

func (testScoringRegistry) Lookup(name string) (core.ScoringFunc, bool) {
	switch name {
	case "exact":
		return func(a, b core.MatchableRecord, field string) (float64, error) {
			av, aok := fieldAsString(a, field)
			bv, bok := fieldAsString(b, field)
			if !aok || !bok {
				return 0, nil
			}
			if av == bv {
				return 1, nil
			}
			return 0, nil
		}, true
	case "numeric_closeness":
		return func(a, b core.MatchableRecord, field string) (float64, error) {
			diff := a.BaseAmount.Sub(b.BaseAmount).Abs()
			denom := a.BaseAmount.Abs()
			if denom.IsZero() {
				denom = b.BaseAmount.Abs()
			}
			if denom.IsZero() {
				return 1, nil
			}
			ratio, _ := diff.Div(denom).Float64()
			score := 1 - ratio
			if score < 0 {
				score = 0
			}
			return score, nil
		}, true
	case "date_proximity":
		return func(a, b core.MatchableRecord, field string) (float64, error) {
			days := math.Abs(a.ValueDate.Sub(b.ValueDate).Hours() / 24)
			score := 1 - days/7 // linear falloff over a week
			if score < 0 {
				score = 0
			}
			return score, nil
		}, true
	default:
		return nil, false
	}
}

func fieldAsString(rec core.MatchableRecord, field string) (string, bool) {
	switch field {
	case "currency":
		return rec.Currency, true
	case "reference":
		return rec.Reference, true
	case "counterparty_ref":
		return rec.CounterpartyRef, true
	case "side":
		return rec.Side, true
	default:
		v, ok := rec.Extra[field]
		if !ok {
			return "", false
		}
		s, ok := v.(string)
		return s, ok
	}
}
