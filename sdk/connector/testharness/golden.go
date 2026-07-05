package testharness

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
)

// CompareGolden diffs records against the JSON array stored at
// goldenPath (golden-file style, matching the conformance-test
// philosophy already used for the built-in MT940/BAI2/ISO20022 format
// parsers - plans/docs/16-development-workflow.md §16.4). Each record
// is compared as normalized JSON (re-marshaled, so key ordering/
// whitespace differences don't cause a false mismatch) rather than a
// raw byte comparison.
func CompareGolden(records [][]byte, goldenPath string) error {
	goldenRaw, err := os.ReadFile(goldenPath)
	if err != nil {
		return fmt.Errorf("testharness: read golden fixture %s: %w", goldenPath, err)
	}

	var golden []json.RawMessage
	if err := json.Unmarshal(goldenRaw, &golden); err != nil {
		return fmt.Errorf("testharness: golden fixture %s is not a JSON array: %w", goldenPath, err)
	}

	if len(records) != len(golden) {
		return fmt.Errorf("testharness: expected %d records per golden fixture %s, got %d", len(golden), goldenPath, len(records))
	}

	for i, rec := range records {
		gotNorm, err := normalizeJSON(rec)
		if err != nil {
			return fmt.Errorf("testharness: record %d is not valid JSON: %w", i, err)
		}
		wantNorm, err := normalizeJSON(golden[i])
		if err != nil {
			return fmt.Errorf("testharness: golden fixture %s record %d is not valid JSON: %w", goldenPath, i, err)
		}
		if !bytes.Equal(gotNorm, wantNorm) {
			return fmt.Errorf("testharness: record %d does not match golden fixture %s:\n  got:  %s\n  want: %s", i, goldenPath, gotNorm, wantNorm)
		}
	}
	return nil
}

// WriteGolden writes records to goldenPath as a JSON array - the
// scaffold's own `jengine-connector test --update-golden` flag (or a
// first-time author establishing their initial fixture) uses this
// rather than hand-authoring expected output.
func WriteGolden(records [][]byte, goldenPath string) error {
	raw := make([]json.RawMessage, len(records))
	for i, r := range records {
		raw[i] = r
	}
	out, err := json.MarshalIndent(raw, "", "  ")
	if err != nil {
		return fmt.Errorf("testharness: marshal golden fixture: %w", err)
	}
	if err := os.WriteFile(goldenPath, out, 0o644); err != nil {
		return fmt.Errorf("testharness: write golden fixture %s: %w", goldenPath, err)
	}
	return nil
}

func normalizeJSON(raw []byte) ([]byte, error) {
	var v any
	if err := json.Unmarshal(raw, &v); err != nil {
		return nil, err
	}
	return json.Marshal(v)
}
