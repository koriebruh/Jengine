package core

import (
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
)

// MatchableRecord is the shared record shape both the batch and streaming
// matchers operate on - a projection of domain.Transaction
// (plans/task/core/05), not a re-derivation of it, so this package
// stays decoupled from the persistence layer per
// plans/docs/13-implementation-notes.md's "single most important
// correctness-critical module" framing (match logic must never drift
// between batch and streaming, which requires exactly one Go type feeding
// both).
type MatchableRecord struct {
	ID              uuid.UUID       `json:"id"`
	TenantID        uuid.UUID       `json:"tenant_id"`
	AccountID       uuid.UUID       `json:"account_id"`
	ValueDate       time.Time       `json:"value_date"`
	BaseAmount      decimal.Decimal `json:"base_amount"` // mirrors Transaction.base_amount numeric(20,4)
	Currency        string          `json:"currency"`
	Reference       string          `json:"reference"`
	CounterpartyRef string          `json:"counterparty_ref"`
	Side            string          `json:"side"`            // DEBIT|CREDIT
	Extra           map[string]any  `json:"extra,omitempty"` // escape hatch for scoring fields not in the fixed set
}

// ToleranceSpec describes how much a BlockingKeyDef's field is allowed to
// vary between a candidate source/target pair and still be bucketed
// together - see engine.go's fan-out logic for how each Kind maps to a
// bounded set of bucket hashes.
type ToleranceSpec struct {
	Kind     string          `yaml:"kind" json:"kind"`         // "exact" | "date_window" | "numeric"
	Days     int             `yaml:"days" json:"days"`         // date_window
	Absolute decimal.Decimal `yaml:"absolute" json:"absolute"` // numeric
	Percent  float64         `yaml:"percent" json:"percent"`   // numeric
}

// BlockingKeyDef names one field used to bucket records before candidate
// generation - see plans/docs/04-matching-engine.md §5.1's keys: list.
type BlockingKeyDef struct {
	Field     string        `yaml:"field" json:"field"` // e.g. "value_date", "base_amount", "currency"
	Tolerance ToleranceSpec `yaml:"tolerance" json:"tolerance"`
}

// ScoringFunc computes a [0,1] similarity for one field between two
// candidate records. Returns an error only for a genuinely invalid
// input (e.g. a field method that requires a numeric field applied to a
// non-numeric one) - a low/zero similarity is a valid computed result,
// not an error.
type ScoringFunc func(a, b MatchableRecord, field string) (float64, error)

// ScoringRegistry is implemented by internal/matching/rules
// (plans/task/core/11), never by this package - core depends on this
// interface, never a concrete registry, so it stays free of any
// dependency on the DSL/parsing package (plans/task/core/10 Common
// Pitfalls: don't put the concrete registry map here "for convenience").
type ScoringRegistry interface {
	Lookup(name string) (ScoringFunc, bool)
}

// ScoringFieldDef names one weighted field comparison in a rule's
// scoring: list.
type ScoringFieldDef struct {
	Field         string  `yaml:"field" json:"field"`
	Method        string  `yaml:"method" json:"method"` // registry key, resolved via ScoringRegistry at match time
	Weight        float64 `yaml:"weight" json:"weight"`
	MinSimilarity float64 `yaml:"min_similarity" json:"min_similarity"`
}

// Cardinality values a CompiledRule may declare. MANY_TO_MANY is
// deliberately not supported by this package - the full subset-sum/
// knapsack aggregation solver it requires is out of MVP scope
// (plans/docs/11-scalability-roadmap.md §12.2 Phase 0); task 11 rejects
// it at compile time before a CompiledRule with this cardinality could
// ever reach Match.
const (
	CardinalityOneToOne  = "ONE_TO_ONE"
	CardinalityOneToMany = "ONE_TO_MANY"
	CardinalityManyToOne = "MANY_TO_ONE"
)

// CompiledRule is the compile *target* both this package and
// plans/task/core/11's DSL compiler agree on - this package never parses
// YAML/JSON into this shape itself (plans/task/core/10 Non-Goals).
type CompiledRule struct {
	ID              uuid.UUID         `yaml:"id" json:"id"`
	TenantID        uuid.UUID         `yaml:"tenant_id" json:"tenant_id"`
	Name            string            `yaml:"name" json:"name"`
	Version         int               `yaml:"version" json:"version"`
	Cardinality     string            `yaml:"cardinality" json:"cardinality"` // ONE_TO_ONE | ONE_TO_MANY | MANY_TO_ONE
	BlockingKeys    []BlockingKeyDef  `yaml:"blocking_keys" json:"blocking_keys"`
	ScoringFields   []ScoringFieldDef `yaml:"scoring_fields" json:"scoring_fields"`
	AutoMatchThresh float64           `yaml:"auto_match_threshold" json:"auto_match_threshold"`
	SuggestThresh   float64           `yaml:"suggest_threshold" json:"suggest_threshold"`
	Priority        int               `yaml:"priority" json:"priority"`             // lower runs first
	MaxGroupSize    int               `yaml:"max_group_size" json:"max_group_size"` // one-to-many bound; MVP hard cap, see grouping.go
}

// ScoredCandidate is one candidate match Match produced, whether it
// ultimately classified as auto-matched or merely suggested.
type ScoredCandidate struct {
	RuleID      uuid.UUID          `json:"rule_id"`
	SourceIDs   []uuid.UUID        `json:"source_ids"`
	TargetIDs   []uuid.UUID        `json:"target_ids"`
	Score       float64            `json:"score"`
	FieldScores map[string]float64 `json:"field_scores"` // per-field breakdown - feeds the "why didn't this match" transparency differentiator (plans/docs/12) and the match-review UI
}

// MatchOutcome is Match's full result: every candidate classified as
// auto-matched or merely suggested, plus whatever's left over after every
// active rule (in priority order) has run. Unmatched residue is not
// automatically a Break, and a Suggested candidate is never a Break
// either - both are the caller's decision (plans/task/core/10 Common
// Pitfalls).
type MatchOutcome struct {
	AutoMatched []ScoredCandidate `json:"auto_matched"`
	Suggested   []ScoredCandidate `json:"suggested"`
	Unmatched   []uuid.UUID       `json:"unmatched"`
}
