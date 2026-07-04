package rules

import (
	"fmt"
	"sync"

	"github.com/shopspring/decimal"

	"github.com/koriebruh/Jengine/internal/matching/core"
	"github.com/koriebruh/Jengine/internal/matching/similarity"
)

// Registry implements core.ScoringRegistry - the concrete scoring-
// function lookup table task 10's core package depends on only as an
// interface, never directly (plans/task/core/10 Common Pitfalls).
type Registry struct {
	mu    sync.RWMutex
	funcs map[string]core.ScoringFunc
}

func NewRegistry() *Registry {
	return &Registry{funcs: make(map[string]core.ScoringFunc)}
}

func (r *Registry) Register(name string, fn core.ScoringFunc) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.funcs[name] = fn
}

func (r *Registry) Lookup(name string) (core.ScoringFunc, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	fn, ok := r.funcs[name]
	return fn, ok
}

// defaultNumericTolerance/defaultDateWindow: core.ScoringFunc's signature
// (func(a, b MatchableRecord, field string) (float64, error), fixed by
// task 10) has no room to pass a per-rule tolerance/window through at
// scoring time - only Compile's BlockingKeyDef.Tolerance carries that,
// and blocking/scoring are intentionally separate concerns. These
// built-ins use a fixed, documented default instead of the rule's own
// configured tolerance; a rule needing scoring-time tolerance different
// from its blocking tolerance should Register a custom closure-
// configured scorer under its own method name.
const (
	defaultNumericTolerance = "0.01"
	defaultDateWindowDays   = 7
)

// DefaultRegistry returns a Registry pre-populated with the MVP built-ins
// plans/docs/04-matching-engine.md §5.1's example references by name:
// exact, jaro_winkler, levenshtein_normalized, numeric_closeness,
// date_proximity.
func DefaultRegistry() *Registry {
	r := NewRegistry()
	r.Register("exact", exactScorer)
	r.Register("jaro_winkler", jaroWinklerScorer)
	r.Register("levenshtein_normalized", levenshteinScorer)
	r.Register("numeric_closeness", numericClosenessScorer)
	r.Register("date_proximity", dateProximityScorer)
	return r
}

func fieldAsString(rec core.MatchableRecord, field string) (string, bool) {
	switch field {
	case "reference":
		return rec.Reference, true
	case "counterparty_ref":
		return rec.CounterpartyRef, true
	case "currency":
		return rec.Currency, true
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

func exactScorer(a, b core.MatchableRecord, field string) (float64, error) {
	av, aok := fieldAsString(a, field)
	bv, bok := fieldAsString(b, field)
	if !aok || !bok {
		return 0, fmt.Errorf("rules: exact: field %q is not a resolvable string field", field)
	}
	if av == bv {
		return 1, nil
	}
	return 0, nil
}

func jaroWinklerScorer(a, b core.MatchableRecord, field string) (float64, error) {
	av, aok := fieldAsString(a, field)
	bv, bok := fieldAsString(b, field)
	if !aok || !bok {
		return 0, fmt.Errorf("rules: jaro_winkler: field %q is not a resolvable string field", field)
	}
	return similarity.JaroWinkler(av, bv), nil
}

func levenshteinScorer(a, b core.MatchableRecord, field string) (float64, error) {
	av, aok := fieldAsString(a, field)
	bv, bok := fieldAsString(b, field)
	if !aok || !bok {
		return 0, fmt.Errorf("rules: levenshtein_normalized: field %q is not a resolvable string field", field)
	}
	return similarity.LevenshteinNormalized(av, bv), nil
}

func numericClosenessScorer(a, b core.MatchableRecord, field string) (float64, error) {
	if field != "base_amount" {
		return 0, fmt.Errorf("rules: numeric_closeness: only supports field \"base_amount\", got %q", field)
	}
	return similarity.NumericCloseness(a.BaseAmount, b.BaseAmount, decimal.RequireFromString(defaultNumericTolerance), 0), nil
}

func dateProximityScorer(a, b core.MatchableRecord, field string) (float64, error) {
	if field != "value_date" {
		return 0, fmt.Errorf("rules: date_proximity: only supports field \"value_date\", got %q", field)
	}
	return similarity.DateProximity(a.ValueDate, b.ValueDate, defaultDateWindowDays), nil
}

var _ core.ScoringRegistry = (*Registry)(nil)
