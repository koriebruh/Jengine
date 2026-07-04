package rules

// RuleStatus mirrors domain.MatchRuleStatus's values - this package
// validates status transitions as a reusable primitive only. It does not
// persist rules, does not implement the maker-checker approval gate, and
// exposes no HTTP endpoints (plans/task/core/11 Implementation Notes) -
// those belong to whatever CRUD/API layer creates and activates
// MatchRule rows (plans/task/core/15).
type RuleStatus string

const (
	RuleStatusDraft    RuleStatus = "DRAFT"
	RuleStatusActive   RuleStatus = "ACTIVE"
	RuleStatusArchived RuleStatus = "ARCHIVED"
)

// validTransitions is the complete allowed-transition table: DRAFT can
// only move to ACTIVE (via approval, enforced elsewhere) or ARCHIVED
// (abandoned before ever activating); ACTIVE can only move to ARCHIVED
// (a rule is never "unarchived" back to DRAFT or ACTIVE - a superseding
// new version is authored instead); ARCHIVED is terminal.
var validTransitions = map[RuleStatus]map[RuleStatus]bool{
	RuleStatusDraft:  {RuleStatusActive: true, RuleStatusArchived: true},
	RuleStatusActive: {RuleStatusArchived: true},
}

// IsValidTransition reports whether moving a MatchRule from status "from"
// to status "to" is allowed. Identity transitions (from == to) are not
// considered valid - a transition implies a change.
func IsValidTransition(from, to RuleStatus) bool {
	return validTransitions[from][to]
}
