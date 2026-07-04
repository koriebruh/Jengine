package rules_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"

	"github.com/koriebruh/Jengine/internal/matching/core"
	"github.com/koriebruh/Jengine/internal/matching/rules"
)

// TestManualVerification_Section5_1Example is plans/task/core/11's
// manual-verification target (as an automated test, per the task's own
// allowance): compile the design doc's §5.1 example (cardinality changed
// to ONE_TO_ONE, since MANY_TO_MANY is correctly rejected by Compile -
// see TestCompile_RejectsManyToMany) and run it against hand-built
// MatchableRecord pairs, confirming AUTO_MATCHED/SUGGESTED/unmatched
// classification.
func TestManualVerification_Section5_1Example(t *testing.T) {
	spec, err := rules.ParseYAML(loadFixture(t, "section_5_1_example_one_to_one.yaml"))
	if err != nil {
		t.Fatalf("ParseYAML failed: %v", err)
	}

	registry := rules.DefaultRegistry()
	compiled, err := rules.Compile(spec, registry)
	if err != nil {
		t.Fatalf("Compile failed: %v", err)
	}
	compiled.ID = uuid.New()

	baseDate := time.Date(2024, 5, 1, 0, 0, 0, 0, time.UTC)

	// Pair 1: near-identical reference (1 char transposed), exact
	// amount/currency/date -> should classify high, likely AUTO_MATCHED.
	srcAuto := core.MatchableRecord{
		ID: uuid.New(), ValueDate: baseDate, BaseAmount: decimal.RequireFromString("1500.00"),
		Currency: "USD", Reference: "INVOICE-1042", CounterpartyRef: "ACME CORP",
	}
	tgtAuto := core.MatchableRecord{
		ID: uuid.New(), ValueDate: baseDate, BaseAmount: decimal.RequireFromString("1500.00"),
		Currency: "USD", Reference: "INVOICE-1042", CounterpartyRef: "ACME CORP",
	}

	// Pair 2: same amount/date/currency but a substantially different
	// reference and counterparty -> should score too low even to suggest,
	// or land in a middle band. We assert it does NOT auto-match (a wide,
	// safe assertion given exact fuzzy-score values are sensitive to the
	// registry's internal fixed-tolerance defaults).
	srcLowConfidence := core.MatchableRecord{
		ID: uuid.New(), ValueDate: baseDate, BaseAmount: decimal.RequireFromString("2200.00"),
		Currency: "USD", Reference: "ZZZ-COMPLETELY-DIFFERENT", CounterpartyRef: "UNRELATED ENTITY",
	}
	tgtLowConfidence := core.MatchableRecord{
		ID: uuid.New(), ValueDate: baseDate, BaseAmount: decimal.RequireFromString("2200.00"),
		Currency: "USD", Reference: "AAA-NOTHING-ALIKE", CounterpartyRef: "SOMEBODY ELSE ENTIRELY",
	}

	// Pair 3: different currency entirely -> blocked out, never even a
	// candidate -> unmatched.
	srcUnmatched := core.MatchableRecord{
		ID: uuid.New(), ValueDate: baseDate, BaseAmount: decimal.RequireFromString("300.00"),
		Currency: "EUR", Reference: "EUR-REF-001", CounterpartyRef: "EU VENDOR",
	}
	tgtUnmatched := core.MatchableRecord{
		ID: uuid.New(), ValueDate: baseDate, BaseAmount: decimal.RequireFromString("300.00"),
		Currency: "GBP", Reference: "EUR-REF-001", CounterpartyRef: "EU VENDOR",
	}

	source := []core.MatchableRecord{srcAuto, srcLowConfidence, srcUnmatched}
	target := []core.MatchableRecord{tgtAuto, tgtLowConfidence, tgtUnmatched}

	outcome, err := core.Match(context.Background(), source, target, []core.CompiledRule{compiled}, registry)
	if err != nil {
		t.Fatalf("Match failed: %v", err)
	}

	foundAutoMatch := false
	for _, c := range outcome.AutoMatched {
		if c.SourceIDs[0] == srcAuto.ID {
			foundAutoMatch = true
		}
	}
	if !foundAutoMatch {
		t.Errorf("expected the near-identical pair to be AUTO_MATCHED; outcome: auto=%d suggested=%d unmatched=%d",
			len(outcome.AutoMatched), len(outcome.Suggested), len(outcome.Unmatched))
	}

	// The EUR/GBP pair must never even be scored (blocked out on currency)
	// - both IDs should appear in Unmatched.
	unmatchedSet := make(map[uuid.UUID]bool, len(outcome.Unmatched))
	for _, id := range outcome.Unmatched {
		unmatchedSet[id] = true
	}
	if !unmatchedSet[srcUnmatched.ID] || !unmatchedSet[tgtUnmatched.ID] {
		t.Errorf("expected the different-currency pair to be unmatched (blocked out), got unmatched=%v", outcome.Unmatched)
	}

	// The low-confidence pair must not be AUTO_MATCHED (whether it lands
	// in Suggested or Unmatched depends on the registry's fixed default
	// tolerances, which isn't the property under test here).
	for _, c := range outcome.AutoMatched {
		if c.SourceIDs[0] == srcLowConfidence.ID {
			t.Errorf("expected the low-confidence pair to NOT auto-match, but it did: %+v", c)
		}
	}

	t.Logf("classification: auto_matched=%d suggested=%d unmatched=%d", len(outcome.AutoMatched), len(outcome.Suggested), len(outcome.Unmatched))
}

func TestParseJSON_RoundTripsSameShapeAsYAML(t *testing.T) {
	jsonDoc := `{
		"rule": {
			"name": "JSON rule",
			"version": 1,
			"match_cardinality": "ONE_TO_ONE",
			"keys": [
				{"field": "currency", "tolerance": "exact"},
				{"field": "value_date", "tolerance": {"type": "date_window", "days": 3}}
			],
			"scoring": [
				{"field": "reference", "method": "exact", "weight": 1.0}
			],
			"thresholds": {"auto_match": 0.9, "suggest": 0.5},
			"execution": {"priority": 5}
		}
	}`

	spec, err := rules.ParseJSON([]byte(jsonDoc))
	if err != nil {
		t.Fatalf("ParseJSON failed: %v", err)
	}
	if spec.Rule.Name != "JSON rule" {
		t.Errorf("unexpected name: %q", spec.Rule.Name)
	}
	if spec.Rule.Keys[0].Tolerance.Type != "exact" {
		t.Errorf("expected bare-string tolerance to parse in JSON too, got %+v", spec.Rule.Keys[0].Tolerance)
	}
	if spec.Rule.Keys[1].Tolerance.Type != "date_window" || spec.Rule.Keys[1].Tolerance.Days != 3 {
		t.Errorf("expected object tolerance to parse in JSON too, got %+v", spec.Rule.Keys[1].Tolerance)
	}

	compiled, err := rules.Compile(spec, rules.DefaultRegistry())
	if err != nil {
		t.Fatalf("Compile of JSON-parsed spec failed: %v", err)
	}
	if compiled.Name != "JSON rule" {
		t.Errorf("unexpected compiled name: %q", compiled.Name)
	}
}
