// Package rules implements the tenant-authored matching-rule DSL:
// parsing plans/docs/04-matching-engine.md §5.1's YAML/JSON shape and
// compiling it into internal/matching/core.CompiledRule, the struct that
// package's Match function executes. See plans/task/core/11.
package rules

import (
	"encoding/json"
	"fmt"
	"math"

	"github.com/shopspring/decimal"
	"gopkg.in/yaml.v3"

	"github.com/koriebruh/Jengine/internal/matching/core"
)

// ToleranceYAML accepts either a bare string ("exact") or an object
// ({type: date_window, days: 2}) - plans/docs/04-matching-engine.md
// §5.1's own example uses both shapes for different keys, so this needs
// a custom unmarshaler for both YAML and JSON (plans/task/core/11
// Implementation Notes).
type ToleranceYAML struct {
	Type     string          `yaml:"type" json:"type"`
	Days     int             `yaml:"days" json:"days"`
	Absolute decimal.Decimal `yaml:"absolute" json:"absolute"`
	Percent  float64         `yaml:"percent" json:"percent"`
}

type toleranceYAMLAlias ToleranceYAML

func (t *ToleranceYAML) UnmarshalYAML(value *yaml.Node) error {
	var bare string
	if err := value.Decode(&bare); err == nil {
		t.Type = bare
		return nil
	}
	var obj toleranceYAMLAlias
	if err := value.Decode(&obj); err != nil {
		return fmt.Errorf("tolerance: expected a bare string or an object: %w", err)
	}
	*t = ToleranceYAML(obj)
	return nil
}

func (t *ToleranceYAML) UnmarshalJSON(data []byte) error {
	var bare string
	if err := json.Unmarshal(data, &bare); err == nil {
		t.Type = bare
		return nil
	}
	var obj toleranceYAMLAlias
	if err := json.Unmarshal(data, &obj); err != nil {
		return fmt.Errorf("tolerance: expected a bare string or an object: %w", err)
	}
	*t = ToleranceYAML(obj)
	return nil
}

type ScopeRef struct {
	AccountGroup string `yaml:"account_group" json:"account_group"`
}

type ScopeSpec struct {
	Source ScopeRef `yaml:"source" json:"source"`
	Target ScopeRef `yaml:"target" json:"target"`
}

type KeySpec struct {
	Field     string        `yaml:"field" json:"field"`
	Tolerance ToleranceYAML `yaml:"tolerance" json:"tolerance"`
}

type ScoringSpec struct {
	Field         string  `yaml:"field" json:"field"`
	Method        string  `yaml:"method" json:"method"`
	Weight        float64 `yaml:"weight" json:"weight"`
	MinSimilarity float64 `yaml:"min_similarity" json:"min_similarity"`
}

type ThresholdSpec struct {
	AutoMatch float64 `yaml:"auto_match" json:"auto_match"`
	Suggest   float64 `yaml:"suggest" json:"suggest"`
}

type SumToleranceSpec struct {
	Absolute decimal.Decimal `yaml:"absolute" json:"absolute"`
}

type AggregationSpec struct {
	MaxGroupSize int              `yaml:"max_group_size" json:"max_group_size"`
	SumTolerance SumToleranceSpec `yaml:"sum_tolerance" json:"sum_tolerance"`
}

type ExecutionSpec struct {
	Priority int      `yaml:"priority" json:"priority"`
	Mode     []string `yaml:"mode" json:"mode"` // ["batch"] and/or ["streaming"] - preserved for task 19's filter, unused until then
}

// RuleSpec mirrors plans/docs/04-matching-engine.md §5.1's YAML shape
// field-for-field - open that doc rather than re-deriving this from
// memory if it ever needs to change.
type RuleSpec struct {
	Rule struct {
		Name             string          `yaml:"name" json:"name"`
		Version          int             `yaml:"version" json:"version"`
		Scope            ScopeSpec       `yaml:"scope" json:"scope"`
		MatchCardinality string          `yaml:"match_cardinality" json:"match_cardinality"`
		Keys             []KeySpec       `yaml:"keys" json:"keys"`
		Scoring          []ScoringSpec   `yaml:"scoring" json:"scoring"`
		Thresholds       ThresholdSpec   `yaml:"thresholds" json:"thresholds"`
		AggregationRules AggregationSpec `yaml:"aggregation_rules" json:"aggregation_rules"`
		Execution        ExecutionSpec   `yaml:"execution" json:"execution"`
	} `yaml:"rule" json:"rule"`
}

func ParseYAML(data []byte) (RuleSpec, error) {
	var spec RuleSpec
	if err := yaml.Unmarshal(data, &spec); err != nil {
		return spec, fmt.Errorf("rules: parse yaml: %w", err)
	}
	return spec, nil
}

func ParseJSON(data []byte) (RuleSpec, error) {
	var spec RuleSpec
	if err := json.Unmarshal(data, &spec); err != nil {
		return spec, fmt.Errorf("rules: parse json: %w", err)
	}
	return spec, nil
}

// Compile turns a parsed RuleSpec into the core.CompiledRule
// internal/matching/core.Match executes. ID/TenantID are left as zero-
// values - whichever persistence layer stores the resulting rule
// (plans/task/core/15) assigns those; Compile is a pure parse-time
// operation with no knowledge of persistence.
func Compile(spec RuleSpec, registry core.ScoringRegistry) (core.CompiledRule, error) {
	r := spec.Rule

	cardinality, err := validateCardinality(r.MatchCardinality)
	if err != nil {
		return core.CompiledRule{}, err
	}

	if len(r.Keys) == 0 {
		return core.CompiledRule{}, fmt.Errorf("rules: compile %q: at least one blocking key is required", r.Name)
	}
	blockingKeys := make([]core.BlockingKeyDef, len(r.Keys))
	for i, k := range r.Keys {
		blockingKeys[i] = core.BlockingKeyDef{
			Field: k.Field,
			Tolerance: core.ToleranceSpec{
				Kind:     normalizeToleranceKind(k.Tolerance.Type),
				Days:     k.Tolerance.Days,
				Absolute: k.Tolerance.Absolute,
				Percent:  k.Tolerance.Percent,
			},
		}
	}

	scoringFields, err := compileScoring(r.Name, r.Scoring, registry)
	if err != nil {
		return core.CompiledRule{}, err
	}

	maxGroupSize := r.AggregationRules.MaxGroupSize
	if maxGroupSize <= 0 || maxGroupSize > core.MaxGroupSizeCap {
		// Clamp rather than reject - plans/task/core/11 Implementation
		// Notes: "do not silently accept a max_group_size: 20 from the
		// doc's own example and pass it straight through if task 10's cap
		// is smaller; keep the two tasks' caps consistent." Clamping
		// (not erroring) keeps the doc's own example compilable while
		// still enforcing the real bound Match will actually use.
		maxGroupSize = core.MaxGroupSizeCap
	}

	return core.CompiledRule{
		Name:            r.Name,
		Version:         r.Version,
		Cardinality:     cardinality,
		BlockingKeys:    blockingKeys,
		ScoringFields:   scoringFields,
		AutoMatchThresh: r.Thresholds.AutoMatch,
		SuggestThresh:   r.Thresholds.Suggest,
		Priority:        r.Execution.Priority,
		MaxGroupSize:    maxGroupSize,
	}, nil
}

func validateCardinality(raw string) (string, error) {
	switch raw {
	case core.CardinalityOneToOne, core.CardinalityOneToMany, core.CardinalityManyToOne:
		return raw, nil
	case "MANY_TO_MANY":
		// Hard compile-time rejection, never a silent downgrade to
		// MANY_TO_ONE/ONE_TO_MANY (plans/task/core/11 Common Pitfalls) -
		// the full subset-sum/knapsack aggregation solver MANY_TO_MANY
		// requires is out of MVP scope (plans/task/core/10 Non-Goals).
		return "", fmt.Errorf("rules: compile: match_cardinality MANY_TO_MANY is not supported (requires the full many-to-many aggregation solver, out of MVP scope)")
	default:
		return "", fmt.Errorf("rules: compile: unrecognized match_cardinality %q", raw)
	}
}

func normalizeToleranceKind(kind string) string {
	if kind == "" {
		return "exact"
	}
	return kind
}

func compileScoring(ruleName string, specs []ScoringSpec, registry core.ScoringRegistry) ([]core.ScoringFieldDef, error) {
	if len(specs) == 0 {
		return nil, fmt.Errorf("rules: compile %q: at least one scoring field is required", ruleName)
	}

	var totalWeight float64
	for _, s := range specs {
		// Fail compilation immediately on an unregistered method - the
		// primary defense against a rule silently no-op'ing on a scoring
		// field because of a typo'd method name (plans/task/core/11
		// Common Pitfalls: this must never surface as a runtime 0-score
		// deep inside a batch run).
		if _, ok := registry.Lookup(s.Method); !ok {
			return nil, fmt.Errorf("rules: compile %q: scoring field %q references unregistered method %q", ruleName, s.Field, s.Method)
		}
		totalWeight += s.Weight
	}

	fields := make([]core.ScoringFieldDef, len(specs))
	needsNormalization := totalWeight > 0 && math.Abs(totalWeight-1.0) > 1e-9
	for i, s := range specs {
		weight := s.Weight
		if needsNormalization {
			// Normalizing (not just warning) is the more forgiving
			// default for tenant-authored YAML (plans/task/core/11
			// Implementation Notes) - without this, weights that don't
			// sum to 1.0 could make auto_match unreachable regardless of
			// match quality, which looks like a matching-engine bug
			// rather than the DSL validation gap it actually is (Common
			// Pitfalls).
			weight = s.Weight / totalWeight
		}
		fields[i] = core.ScoringFieldDef{
			Field: s.Field, Method: s.Method, Weight: weight, MinSimilarity: s.MinSimilarity,
		}
	}
	return fields, nil
}
