package core_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/koriebruh/Jengine/internal/matching/core"
)

// Golden-dataset runner per plans/task/core/17 and
// plans/docs/16-development-workflow.md §16.4. Walks testdata/<case>/,
// loads source.json/target.json/rules.yaml/expected.json, runs
// core.Match, and diffs against expected.json.
//
// This convention/runner is task 17's deliverable - see
// testdata/README.md. Task 10 populates the real fixture set and is
// responsible for keeping it green.

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

			rules, err := os.ReadFile(filepath.Join(dir, "rules.yaml"))
			if err != nil {
				t.Fatalf("failed to read rules.yaml: %v", err)
			}

			expectedBytes, err := os.ReadFile(filepath.Join(dir, "expected.json"))
			if err != nil {
				t.Fatalf("failed to read expected.json: %v", err)
			}
			var expected core.MatchOutcome
			if err := json.Unmarshal(expectedBytes, &expected); err != nil {
				t.Fatalf("failed to parse expected.json: %v", err)
			}

			actual := core.Match(source, target, rules)

			if !reflect.DeepEqual(actual, expected) {
				t.Fatalf("golden mismatch for case %q:\n  got:      %+v\n  expected: %+v", caseName, actual, expected)
			}
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
