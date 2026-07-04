package rules_test

import (
	"os"
	"testing"

	"github.com/shopspring/decimal"

	"github.com/koriebruh/Jengine/internal/matching/core"
	"github.com/koriebruh/Jengine/internal/matching/rules"
)

func loadFixture(t *testing.T, name string) []byte {
	t.Helper()
	data, err := os.ReadFile("testdata/" + name)
	if err != nil {
		t.Fatalf("read fixture %s: %v", name, err)
	}
	return data
}

func TestParseYAML_Section5_1Example(t *testing.T) {
	spec, err := rules.ParseYAML(loadFixture(t, "section_5_1_example.yaml"))
	if err != nil {
		t.Fatalf("ParseYAML failed: %v", err)
	}

	r := spec.Rule
	if r.Name != "Bank vs GL - Standard Match" {
		t.Errorf("unexpected name: %q", r.Name)
	}
	if r.Version != 3 {
		t.Errorf("unexpected version: %d", r.Version)
	}
	if r.MatchCardinality != "MANY_TO_MANY" {
		t.Errorf("unexpected cardinality: %q", r.MatchCardinality)
	}
	if r.Scope.Source.AccountGroup != "bank_accounts" || r.Scope.Target.AccountGroup != "gl_cash_accounts" {
		t.Errorf("unexpected scope: %+v", r.Scope)
	}

	if len(r.Keys) != 3 {
		t.Fatalf("expected 3 blocking keys, got %d", len(r.Keys))
	}
	// key 1: object-shaped tolerance
	if r.Keys[0].Field != "value_date" || r.Keys[0].Tolerance.Type != "date_window" || r.Keys[0].Tolerance.Days != 2 {
		t.Errorf("unexpected key[0]: %+v", r.Keys[0])
	}
	// key 2: object-shaped tolerance with absolute/percent
	if r.Keys[1].Field != "base_amount" || r.Keys[1].Tolerance.Type != "numeric" || !r.Keys[1].Tolerance.Absolute.Equal(decimal.RequireFromString("0.01")) {
		t.Errorf("unexpected key[1]: %+v", r.Keys[1])
	}
	// key 3: bare-string tolerance ("exact")
	if r.Keys[2].Field != "currency" || r.Keys[2].Tolerance.Type != "exact" {
		t.Errorf("unexpected key[2] (bare-string tolerance): %+v", r.Keys[2])
	}

	if len(r.Scoring) != 4 {
		t.Fatalf("expected 4 scoring fields, got %d", len(r.Scoring))
	}
	if r.Scoring[0].Method != "jaro_winkler" || r.Scoring[0].Weight != 0.4 || r.Scoring[0].MinSimilarity != 0.75 {
		t.Errorf("unexpected scoring[0]: %+v", r.Scoring[0])
	}
	if r.Scoring[1].Method != "levenshtein_normalized" {
		t.Errorf("unexpected scoring[1] method: %q", r.Scoring[1].Method)
	}

	if r.Thresholds.AutoMatch != 0.92 || r.Thresholds.Suggest != 0.65 {
		t.Errorf("unexpected thresholds: %+v", r.Thresholds)
	}
	if r.AggregationRules.MaxGroupSize != 20 {
		t.Errorf("unexpected max_group_size: %d", r.AggregationRules.MaxGroupSize)
	}
	if r.Execution.Priority != 10 || len(r.Execution.Mode) != 2 {
		t.Errorf("unexpected execution: %+v", r.Execution)
	}
}

func TestParseYAML_MinimalRuleMissingOptionalFields(t *testing.T) {
	spec, err := rules.ParseYAML(loadFixture(t, "minimal_rule.yaml"))
	if err != nil {
		t.Fatalf("ParseYAML failed: %v", err)
	}
	r := spec.Rule
	if r.Name == "" || r.MatchCardinality != "ONE_TO_ONE" {
		t.Errorf("unexpected parse of minimal rule: %+v", r)
	}
	// Optional fields left at zero values, not an error.
	if r.AggregationRules.MaxGroupSize != 0 {
		t.Errorf("expected zero-value max_group_size when omitted, got %d", r.AggregationRules.MaxGroupSize)
	}
}

func TestCompile_RejectsManyToMany(t *testing.T) {
	spec, err := rules.ParseYAML(loadFixture(t, "section_5_1_example.yaml"))
	if err != nil {
		t.Fatalf("ParseYAML failed: %v", err)
	}

	_, err = rules.Compile(spec, rules.DefaultRegistry())
	if err == nil {
		t.Fatal("expected Compile to reject MANY_TO_MANY cardinality")
	}
}

func TestCompile_UnregisteredMethodFailsFast(t *testing.T) {
	spec := rules.RuleSpec{}
	spec.Rule.Name = "bad rule"
	spec.Rule.MatchCardinality = "ONE_TO_ONE"
	spec.Rule.Keys = []rules.KeySpec{{Field: "currency", Tolerance: rules.ToleranceYAML{Type: "exact"}}}
	spec.Rule.Scoring = []rules.ScoringSpec{{Field: "reference", Method: "does_not_exist", Weight: 1.0}}
	spec.Rule.Thresholds = rules.ThresholdSpec{AutoMatch: 0.9, Suggest: 0.5}

	_, err := rules.Compile(spec, rules.DefaultRegistry())
	if err == nil {
		t.Fatal("expected Compile to fail fast on an unregistered scoring method")
	}
}

func TestCompile_WeightNormalization(t *testing.T) {
	spec := rules.RuleSpec{}
	spec.Rule.Name = "unnormalized weights"
	spec.Rule.MatchCardinality = "ONE_TO_ONE"
	spec.Rule.Keys = []rules.KeySpec{{Field: "currency", Tolerance: rules.ToleranceYAML{Type: "exact"}}}
	spec.Rule.Scoring = []rules.ScoringSpec{
		{Field: "reference", Method: "exact", Weight: 2.0},
		{Field: "currency", Method: "exact", Weight: 2.0},
	}
	spec.Rule.Thresholds = rules.ThresholdSpec{AutoMatch: 0.9, Suggest: 0.5}

	compiled, err := rules.Compile(spec, rules.DefaultRegistry())
	if err != nil {
		t.Fatalf("Compile failed: %v", err)
	}
	var total float64
	for _, f := range compiled.ScoringFields {
		total += f.Weight
	}
	if total < 0.999 || total > 1.001 {
		t.Errorf("expected normalized weights to sum to ~1.0, got %v", total)
	}
	// Original weights were equal (2.0, 2.0), so normalized should stay
	// equal (0.5, 0.5).
	if compiled.ScoringFields[0].Weight != 0.5 || compiled.ScoringFields[1].Weight != 0.5 {
		t.Errorf("expected equal weights to normalize to 0.5/0.5, got %+v", compiled.ScoringFields)
	}
}

func TestCompile_MaxGroupSizeClamping(t *testing.T) {
	spec, err := rules.ParseYAML(loadFixture(t, "section_5_1_example_one_to_one.yaml"))
	if err != nil {
		t.Fatalf("ParseYAML failed: %v", err)
	}
	// The doc's own example uses max_group_size: 20, above
	// core.MaxGroupSizeCap - must be clamped, not passed straight through.
	compiled, err := rules.Compile(spec, rules.DefaultRegistry())
	if err != nil {
		t.Fatalf("Compile failed: %v", err)
	}
	if compiled.MaxGroupSize != core.MaxGroupSizeCap {
		t.Errorf("expected max_group_size clamped to %d, got %d", core.MaxGroupSizeCap, compiled.MaxGroupSize)
	}
}

func TestCompile_CardinalityValidation(t *testing.T) {
	cases := []struct {
		cardinality string
		wantErr     bool
	}{
		{"ONE_TO_ONE", false},
		{"ONE_TO_MANY", false},
		{"MANY_TO_ONE", false},
		{"MANY_TO_MANY", true},
		{"NOT_A_REAL_CARDINALITY", true},
	}
	for _, c := range cases {
		t.Run(c.cardinality, func(t *testing.T) {
			spec := rules.RuleSpec{}
			spec.Rule.Name = "test"
			spec.Rule.MatchCardinality = c.cardinality
			spec.Rule.Keys = []rules.KeySpec{{Field: "currency", Tolerance: rules.ToleranceYAML{Type: "exact"}}}
			spec.Rule.Scoring = []rules.ScoringSpec{{Field: "reference", Method: "exact", Weight: 1.0}}
			spec.Rule.Thresholds = rules.ThresholdSpec{AutoMatch: 0.9, Suggest: 0.5}

			_, err := rules.Compile(spec, rules.DefaultRegistry())
			if c.wantErr && err == nil {
				t.Errorf("expected an error for cardinality %q, got nil", c.cardinality)
			}
			if !c.wantErr && err != nil {
				t.Errorf("expected no error for cardinality %q, got %v", c.cardinality, err)
			}
		})
	}
}

func TestCompile_MissingKeysOrScoringFails(t *testing.T) {
	t.Run("no blocking keys", func(t *testing.T) {
		spec := rules.RuleSpec{}
		spec.Rule.MatchCardinality = "ONE_TO_ONE"
		spec.Rule.Scoring = []rules.ScoringSpec{{Field: "reference", Method: "exact", Weight: 1.0}}
		_, err := rules.Compile(spec, rules.DefaultRegistry())
		if err == nil {
			t.Fatal("expected an error for a rule with no blocking keys")
		}
	})

	t.Run("no scoring fields", func(t *testing.T) {
		spec := rules.RuleSpec{}
		spec.Rule.MatchCardinality = "ONE_TO_ONE"
		spec.Rule.Keys = []rules.KeySpec{{Field: "currency", Tolerance: rules.ToleranceYAML{Type: "exact"}}}
		_, err := rules.Compile(spec, rules.DefaultRegistry())
		if err == nil {
			t.Fatal("expected an error for a rule with no scoring fields")
		}
	})
}
