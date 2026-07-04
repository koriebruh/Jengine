package core

// MatchableRecord is a placeholder shape for a source/target record used
// by the golden-dataset runner (plans/task/core/17). Task 10 defines the
// real Transaction-derived matchable-record shape per
// plans/docs/04-matching-engine.md §5.1-5.2 - this placeholder exists
// only so the golden runner has something concrete to load/diff before
// task 10 lands, and will be superseded there.
type MatchableRecord struct {
	ID        string  `json:"id"`
	Amount    float64 `json:"amount"`
	Reference string  `json:"reference"`
}

// MatchOutcome is a placeholder shape for the result of a matching run.
// Task 10 defines the real shape (MatchResult/MatchResultLine per
// plans/docs/03-canonical-data-model.md §4.1) - superseded there.
type MatchOutcome struct {
	AutoMatched     [][2]string `json:"auto_matched"`
	Suggested       [][2]string `json:"suggested"`
	UnmatchedSource []string    `json:"unmatched_source"`
	UnmatchedTarget []string    `json:"unmatched_target"`
}

// Match is a placeholder implementation that always returns an empty
// outcome, regardless of input. Task 10 REPLACES this function body with
// the real blocking-key + scoring engine
// (plans/docs/04-matching-engine.md §5.2) - the golden-dataset runner
// (golden_test.go) calls this exact function by this exact name, so
// task 10's real implementation is proven against the fixtures without
// the runner itself needing to change.
func Match(source, target []MatchableRecord, rules []byte) MatchOutcome {
	return MatchOutcome{
		AutoMatched:     [][2]string{},
		Suggested:       [][2]string{},
		UnmatchedSource: []string{},
		UnmatchedTarget: []string{},
	}
}
