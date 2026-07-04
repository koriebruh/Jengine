package core_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"

	"github.com/koriebruh/Jengine/internal/matching/core"
)

func mkRecord(id uuid.UUID, date time.Time, amount string, currency string) core.MatchableRecord {
	return core.MatchableRecord{
		ID: id, ValueDate: date, BaseAmount: decimal.RequireFromString(amount), Currency: currency,
	}
}

func TestBuildBlockingIndex_ExactTolerance(t *testing.T) {
	r1 := mkRecord(uuid.New(), time.Now(), "100.00", "USD")
	r2 := mkRecord(uuid.New(), time.Now(), "200.00", "EUR")
	idx := core.BuildBlockingIndex([]core.MatchableRecord{r1, r2}, []core.BlockingKeyDef{
		{Field: "currency", Tolerance: core.ToleranceSpec{Kind: "exact"}},
	})

	// Exact tolerance: each record occupies exactly 1 bucket, so 2
	// distinct-currency records produce exactly 2 buckets - no fan-out.
	if got := idx.BucketCount(); got != 2 {
		t.Errorf("expected 2 buckets for 2 distinct exact-tolerance values, got %d", got)
	}

	candidates := idx.CandidatesFor(&r1)
	if len(candidates) != 1 || candidates[0].ID != r1.ID {
		t.Errorf("expected exactly r1 as its own candidate, got %+v", candidates)
	}
}

func TestBuildBlockingIndex_DateWindowFanOut(t *testing.T) {
	base := time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC)
	r1 := mkRecord(uuid.New(), base, "100.00", "USD")

	const days = 2
	idx := core.BuildBlockingIndex([]core.MatchableRecord{r1}, []core.BlockingKeyDef{
		{Field: "value_date", Tolerance: core.ToleranceSpec{Kind: "date_window", Days: days}},
	})

	// A single record with a ±N day window must fan out into exactly
	// 2N+1 buckets (plans/task/core/10 Implementation Notes) - bounded,
	// not unbounded.
	wantBuckets := 2*days + 1
	if got := idx.BucketCount(); got != wantBuckets {
		t.Errorf("expected %d buckets (2*%d+1) for a date_window(days=%d) record, got %d", wantBuckets, days, days, got)
	}

	// A record exactly `days` away must still be found as a candidate -
	// proving the fan-out actually achieves its purpose, not just the
	// right count.
	nearby := mkRecord(uuid.New(), base.AddDate(0, 0, days), "999.00", "USD")
	idx2 := core.BuildBlockingIndex([]core.MatchableRecord{r1}, []core.BlockingKeyDef{
		{Field: "value_date", Tolerance: core.ToleranceSpec{Kind: "date_window", Days: days}},
	})
	candidates := idx2.CandidatesFor(&nearby)
	if len(candidates) != 1 {
		t.Fatalf("expected the record %d days away to be found as a candidate, got %d candidates", days, len(candidates))
	}

	// A record beyond the window must NOT be found.
	tooFar := mkRecord(uuid.New(), base.AddDate(0, 0, days+10), "999.00", "USD")
	candidatesFar := idx2.CandidatesFor(&tooFar)
	if len(candidatesFar) != 0 {
		t.Errorf("expected no candidates for a record far outside the date window, got %d", len(candidatesFar))
	}
}

func TestBuildBlockingIndex_NumericToleranceFanOut(t *testing.T) {
	r1 := mkRecord(uuid.New(), time.Now(), "100.00", "USD")

	idx := core.BuildBlockingIndex([]core.MatchableRecord{r1}, []core.BlockingKeyDef{
		{Field: "base_amount", Tolerance: core.ToleranceSpec{Kind: "numeric", Absolute: decimal.RequireFromString("1.00")}},
	})

	// Numeric tolerance fans out into a small bounded set (own bucket ±1
	// neighbor) - never unbounded.
	if got := idx.BucketCount(); got > 3 {
		t.Errorf("expected numeric tolerance fan-out to stay <= 3 buckets, got %d", got)
	}

	// A record within tolerance must be found.
	nearby := mkRecord(uuid.New(), time.Now(), "100.50", "USD")
	candidates := idx.CandidatesFor(&nearby)
	if len(candidates) != 1 {
		t.Errorf("expected a record within numeric tolerance to be found as a candidate, got %d", len(candidates))
	}
}

func TestBuildBlockingIndex_FanOutStaysBoundedAcrossManyRecords(t *testing.T) {
	// The whole point of blocking: fan-out per record must not grow with
	// N. Build an index over 500 records and assert bucket count stays
	// roughly proportional to N (a handful of buckets per record), not
	// exploding.
	records := make([]core.MatchableRecord, 500)
	base := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	for i := range records {
		records[i] = mkRecord(uuid.New(), base.AddDate(0, 0, i%30), "100.00", "USD")
	}

	idx := core.BuildBlockingIndex(records, []core.BlockingKeyDef{
		{Field: "value_date", Tolerance: core.ToleranceSpec{Kind: "date_window", Days: 1}},
	})

	// 30 distinct days, ±1 day fan-out (3 buckets/record) -> at most ~32
	// distinct buckets total (30 days + 2 edge days), nowhere near
	// N=500 or N^2=250000.
	if got := idx.BucketCount(); got > 40 {
		t.Errorf("expected bucket count to stay small (~30-40) regardless of N=500 records, got %d", got)
	}
}

type fakeRegistry struct{ fns map[string]core.ScoringFunc }

func (r fakeRegistry) Lookup(name string) (core.ScoringFunc, bool) {
	fn, ok := r.fns[name]
	return fn, ok
}

func exactScorer(field string) core.ScoringFunc {
	return func(a, b core.MatchableRecord, f string) (float64, error) {
		if f == "currency" {
			if a.Currency == b.Currency {
				return 1, nil
			}
			return 0, nil
		}
		if f == "reference" {
			if a.Reference == b.Reference {
				return 1, nil
			}
			return 0, nil
		}
		return 0, nil
	}
}

func TestMatch_ThresholdClassificationBoundaries(t *testing.T) {
	registry := fakeRegistry{fns: map[string]core.ScoringFunc{
		"exact": exactScorer("currency"),
	}}

	newRule := func(auto, suggest float64) core.CompiledRule {
		return core.CompiledRule{
			ID: uuid.New(), Cardinality: core.CardinalityOneToOne,
			BlockingKeys:    []core.BlockingKeyDef{{Field: "currency", Tolerance: core.ToleranceSpec{Kind: "exact"}}},
			ScoringFields:   []core.ScoringFieldDef{{Field: "currency", Method: "exact", Weight: 1}},
			AutoMatchThresh: auto, SuggestThresh: suggest, Priority: 1,
		}
	}

	t.Run("score exactly at auto_match threshold classifies as auto-matched", func(t *testing.T) {
		src := []core.MatchableRecord{{ID: uuid.New(), Currency: "USD"}}
		tgt := []core.MatchableRecord{{ID: uuid.New(), Currency: "USD"}}
		outcome, err := core.Match(context.Background(), src, tgt, []core.CompiledRule{newRule(1.0, 0.5)}, registry)
		if err != nil {
			t.Fatalf("Match failed: %v", err)
		}
		if len(outcome.AutoMatched) != 1 {
			t.Errorf("expected score==threshold to classify as auto-matched, got %+v", outcome)
		}
	})

	t.Run("score exactly at suggest threshold (below auto) classifies as suggested", func(t *testing.T) {
		src := []core.MatchableRecord{{ID: uuid.New(), Currency: "USD"}}
		tgt := []core.MatchableRecord{{ID: uuid.New(), Currency: "USD"}}
		// exact scorer returns 1.0 always for a currency match, so set
		// auto threshold above 1.0 (unreachable) to force suggested
		// classification at score=1.0 >= suggest=1.0.
		outcome, err := core.Match(context.Background(), src, tgt, []core.CompiledRule{newRule(1.5, 1.0)}, registry)
		if err != nil {
			t.Fatalf("Match failed: %v", err)
		}
		if len(outcome.Suggested) != 1 {
			t.Errorf("expected score==suggest threshold (below auto) to classify as suggested, got %+v", outcome)
		}
	})

	t.Run("score below suggest threshold is unmatched", func(t *testing.T) {
		src := []core.MatchableRecord{{ID: uuid.New(), Currency: "USD"}}
		tgt := []core.MatchableRecord{{ID: uuid.New(), Currency: "EUR"}} // no currency match -> blocking excludes it entirely
		outcome, err := core.Match(context.Background(), src, tgt, []core.CompiledRule{newRule(0.9, 0.5)}, registry)
		if err != nil {
			t.Fatalf("Match failed: %v", err)
		}
		if len(outcome.AutoMatched) != 0 || len(outcome.Suggested) != 0 {
			t.Errorf("expected no match for a blocked-out pair, got %+v", outcome)
		}
		if len(outcome.Unmatched) != 2 {
			t.Errorf("expected both records unmatched, got %+v", outcome.Unmatched)
		}
	})
}

func TestMatch_PriorityChainingRemovesMatchedRecordsBetweenRules(t *testing.T) {
	registry := fakeRegistry{fns: map[string]core.ScoringFunc{
		"exact": exactScorer("reference"),
	}}

	srcID, tgtID := uuid.New(), uuid.New()
	src := core.MatchableRecord{ID: srcID, Currency: "USD", Reference: "REF-A"}
	tgt := core.MatchableRecord{ID: tgtID, Currency: "USD", Reference: "REF-A"}

	// Two rules with the SAME blocking/scoring config but different
	// priorities - if rule-priority chaining works, rule 1 (lower
	// Priority number = runs first) claims the pair, and rule 2 (higher
	// Priority number) never gets a chance to also "claim" it (there's
	// nothing left in its input pool).
	rule1 := core.CompiledRule{
		ID: uuid.New(), Cardinality: core.CardinalityOneToOne,
		BlockingKeys:    []core.BlockingKeyDef{{Field: "currency", Tolerance: core.ToleranceSpec{Kind: "exact"}}},
		ScoringFields:   []core.ScoringFieldDef{{Field: "reference", Method: "exact", Weight: 1}},
		AutoMatchThresh: 0.9, SuggestThresh: 0.5, Priority: 1,
	}
	rule2 := rule1
	rule2.ID = uuid.New()
	rule2.Priority = 2

	outcome, err := core.Match(context.Background(), []core.MatchableRecord{src}, []core.MatchableRecord{tgt}, []core.CompiledRule{rule2, rule1}, registry)
	if err != nil {
		t.Fatalf("Match failed: %v", err)
	}

	if len(outcome.AutoMatched) != 1 {
		t.Fatalf("expected exactly 1 auto-matched pair, got %d: %+v", len(outcome.AutoMatched), outcome.AutoMatched)
	}
	if outcome.AutoMatched[0].RuleID != rule1.ID {
		t.Errorf("expected rule1 (lower Priority number, runs first) to claim the match, got rule %s", outcome.AutoMatched[0].RuleID)
	}
	if len(outcome.Unmatched) != 0 {
		t.Errorf("expected no unmatched residue, got %+v", outcome.Unmatched)
	}
}

// TestMatch_CandidateCountStaysNearLinear is plans/task/core/10's manual-
// verification sanity check: ~50 synthetic records across a few
// blocking-key configurations, confirming candidate generation doesn't
// degrade toward O(N×M) (the regression this whole package exists to
// prevent).
func TestMatch_CandidateCountStaysNearLinear(t *testing.T) {
	const n = 50
	base := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

	records := make([]core.MatchableRecord, n)
	for i := range records {
		records[i] = core.MatchableRecord{
			ID:         uuid.New(),
			ValueDate:  base.AddDate(0, 0, i%10),
			BaseAmount: decimal.NewFromInt(int64(100 + i%20)),
			Currency:   []string{"USD", "EUR", "GBP"}[i%3],
		}
	}

	idx := core.BuildBlockingIndex(records, []core.BlockingKeyDef{
		{Field: "currency", Tolerance: core.ToleranceSpec{Kind: "exact"}},
		{Field: "value_date", Tolerance: core.ToleranceSpec{Kind: "date_window", Days: 1}},
		{Field: "base_amount", Tolerance: core.ToleranceSpec{Kind: "numeric", Absolute: decimal.NewFromInt(1)}},
	})

	totalCandidates := 0
	for i := range records {
		totalCandidates += len(idx.CandidatesFor(&records[i]))
	}

	// O(N×M) over 50 records would be up to 2500 candidate-record pairs;
	// near-linear blocking should stay a small multiple of N, not
	// anywhere close to N^2.
	avgCandidatesPerRecord := float64(totalCandidates) / float64(n)
	if avgCandidatesPerRecord > 15 {
		t.Errorf("average candidates per record (%.1f) suggests blocking isn't narrowing the search - expected a small number, not approaching N=%d", avgCandidatesPerRecord, n)
	}
	t.Logf("n=%d, total candidate pairs=%d, avg candidates/record=%.2f (O(N^2) would be ~%d)", n, totalCandidates, avgCandidatesPerRecord, n*n)
}
