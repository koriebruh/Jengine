package core

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
)

// BlockingIndex buckets records by a composite hash of BlockingKeyDef
// fields, fanning tolerance-bearing keys out over a small bounded set of
// buckets so two records within tolerance of each other are guaranteed
// to share at least one bucket - see bucketValuesForField for the
// per-Kind fan-out logic. This is what keeps candidate generation
// O(N × fan_out) instead of O(N×M) (plans/task/core/10 Common Pitfalls).
type BlockingIndex struct {
	buckets map[string][]int
	records []MatchableRecord
	keys    []BlockingKeyDef
}

// BuildBlockingIndex indexes records under keys.
func BuildBlockingIndex(records []MatchableRecord, keys []BlockingKeyDef) *BlockingIndex {
	idx := &BlockingIndex{
		buckets: make(map[string][]int),
		records: records,
		keys:    keys,
	}
	for i := range records {
		for _, bk := range compositeBucketKeys(&records[i], keys) {
			idx.buckets[bk] = append(idx.buckets[bk], i)
		}
	}
	return idx
}

// CandidatesFor returns every indexed record sharing at least one bucket
// with query, deduplicated. Bounded by the same fan-out that bounded
// indexing - never a full scan of the index.
func (idx *BlockingIndex) CandidatesFor(query *MatchableRecord) []MatchableRecord {
	seen := make(map[int]bool)
	var out []MatchableRecord
	for _, bk := range compositeBucketKeys(query, idx.keys) {
		for _, i := range idx.buckets[bk] {
			if seen[i] {
				continue
			}
			seen[i] = true
			out = append(out, idx.records[i])
		}
	}
	return out
}

// BucketCount reports how many distinct buckets exist - exposed for
// tests asserting fan-out stays bounded, not part of the matching
// algorithm itself.
func (idx *BlockingIndex) BucketCount() int { return len(idx.buckets) }

// compositeBucketKeys returns every composite bucket key rec maps to
// under keys - the cartesian product of each individual key's fanned-out
// bucket values, joined into single strings. Bounded because each
// individual key's fan-out is itself bounded (a handful of buckets), so
// even a cartesian product across 2-3 keys stays small.
func compositeBucketKeys(rec *MatchableRecord, keys []BlockingKeyDef) []string {
	if len(keys) == 0 {
		return []string{"*"}
	}

	combos := [][]string{{}}
	for _, k := range keys {
		values := bucketValuesForField(rec, k)
		next := make([][]string, 0, len(combos)*len(values))
		for _, combo := range combos {
			for _, v := range values {
				nc := make([]string, len(combo), len(combo)+1)
				copy(nc, combo)
				nc = append(nc, k.Field+"="+v)
				next = append(next, nc)
			}
		}
		combos = next
	}

	out := make([]string, len(combos))
	for i, combo := range combos {
		out[i] = strings.Join(combo, "|")
	}
	return out
}

// bucketValuesForField returns the bounded set of bucket-value strings
// rec's field maps to under key's tolerance.
func bucketValuesForField(rec *MatchableRecord, key BlockingKeyDef) []string {
	switch key.Tolerance.Kind {
	case "date_window":
		return dateWindowBuckets(rec, key)
	case "numeric":
		return numericBuckets(rec, key)
	default: // "exact" or unset
		return []string{exactBucketValue(rec, key.Field)}
	}
}

func dateWindowBuckets(rec *MatchableRecord, key BlockingKeyDef) []string {
	t, ok := timeFieldValue(rec, key.Field)
	if !ok {
		return []string{"invalid:" + key.Field}
	}
	days := key.Tolerance.Days
	buckets := make([]string, 0, 2*days+1)
	for d := -days; d <= days; d++ {
		buckets = append(buckets, t.AddDate(0, 0, d).Format("2006-01-02"))
	}
	return buckets
}

func numericBuckets(rec *MatchableRecord, key BlockingKeyDef) []string {
	value, ok := decimalFieldValue(rec, key.Field)
	if !ok {
		return []string{"invalid:" + key.Field}
	}
	width := numericBucketWidth(value, key.Tolerance)
	if width.IsZero() {
		width = decimal.NewFromInt(1)
	}

	center := value.Div(width).Round(0)
	buckets := make([]string, 0, 3)
	for d := -1; d <= 1; d++ {
		idx := center.Add(decimal.NewFromInt(int64(d)))
		buckets = append(buckets, "n:"+idx.String())
	}
	return buckets
}

func numericBucketWidth(value decimal.Decimal, tol ToleranceSpec) decimal.Decimal {
	if !tol.Absolute.IsZero() {
		return tol.Absolute.Mul(decimal.NewFromInt(2))
	}
	if tol.Percent > 0 {
		pct := decimal.NewFromFloat(tol.Percent)
		return value.Abs().Mul(pct).Mul(decimal.NewFromInt(2))
	}
	return decimal.Zero
}

func exactBucketValue(rec *MatchableRecord, field string) string {
	v, ok := fieldValue(rec, field)
	if !ok {
		return "missing:" + field
	}
	switch t := v.(type) {
	case string:
		return t
	case decimal.Decimal:
		return t.String()
	case uuid.UUID:
		return t.String()
	default:
		return fmt.Sprintf("%v", t)
	}
}

// fieldValue resolves a MatchableRecord field by name, checking the
// fixed field set first and falling back to Extra.
func fieldValue(rec *MatchableRecord, field string) (any, bool) {
	switch field {
	case "value_date":
		return rec.ValueDate, true
	case "base_amount":
		return rec.BaseAmount, true
	case "currency":
		return rec.Currency, true
	case "reference":
		return rec.Reference, true
	case "counterparty_ref":
		return rec.CounterpartyRef, true
	case "side":
		return rec.Side, true
	case "account_id":
		return rec.AccountID, true
	default:
		v, ok := rec.Extra[field]
		return v, ok
	}
}

func timeFieldValue(rec *MatchableRecord, field string) (time.Time, bool) {
	v, ok := fieldValue(rec, field)
	if !ok {
		return time.Time{}, false
	}
	t, ok := v.(time.Time)
	return t, ok
}

func decimalFieldValue(rec *MatchableRecord, field string) (decimal.Decimal, bool) {
	v, ok := fieldValue(rec, field)
	if !ok {
		return decimal.Decimal{}, false
	}
	d, ok := v.(decimal.Decimal)
	return d, ok
}

// scoreCandidate computes rule's weighted composite score between source
// and target, plus the per-field breakdown ScoredCandidate.FieldScores
// exposes for the "why didn't this match" transparency differentiator.
func scoreCandidate(source, target MatchableRecord, rule CompiledRule, registry ScoringRegistry) (float64, map[string]float64, error) {
	fieldScores := make(map[string]float64, len(rule.ScoringFields))
	var weightedSum, totalWeight float64

	for _, sf := range rule.ScoringFields {
		fn, ok := registry.Lookup(sf.Method)
		if !ok {
			return 0, nil, fmt.Errorf("core: scoring method %q is not registered", sf.Method)
		}
		score, err := fn(source, target, sf.Field)
		if err != nil {
			return 0, nil, fmt.Errorf("core: score field %q via %q: %w", sf.Field, sf.Method, err)
		}
		if sf.MinSimilarity > 0 && score < sf.MinSimilarity {
			score = 0
		}
		fieldScores[sf.Field] = score
		weightedSum += score * sf.Weight
		totalWeight += sf.Weight
	}

	if totalWeight == 0 {
		return 0, fieldScores, nil
	}
	return weightedSum / totalWeight, fieldScores, nil
}

// Match is the top-level entrypoint both the batch worker
// (plans/task/core/12) and the streaming consumer
// (plans/task/core/19, V1) call - the one place rule-priority chaining
// lives, so match logic never drifts between the two paths
// (plans/docs/13-implementation-notes.md). For each CompiledRule in
// ascending Priority order: build a fresh blocking index over records not
// yet matched by an earlier rule, generate candidates, score, classify
// against AutoMatchThresh/SuggestThresh, and remove matched records from
// the pool before moving to the next rule. Records still unmatched after
// the last rule become MatchOutcome.Unmatched.
//
// Match has no side effects on any table it doesn't own - persistence is
// entirely the caller's job (plans/task/core/10 Common Pitfalls).
func Match(ctx context.Context, source, target []MatchableRecord, rules []CompiledRule, registry ScoringRegistry) (MatchOutcome, error) {
	sorted := make([]CompiledRule, len(rules))
	copy(sorted, rules)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Priority < sorted[j].Priority })

	remainingSource := source
	remainingTarget := target
	var outcome MatchOutcome

	for _, rule := range sorted {
		if err := ctx.Err(); err != nil {
			return outcome, err
		}

		idx := BuildBlockingIndex(remainingTarget, rule.BlockingKeys)
		matchedSource := make(map[uuid.UUID]bool)
		matchedTarget := make(map[uuid.UUID]bool)

		for i := range remainingSource {
			src := remainingSource[i]
			candidates := idx.CandidatesFor(&src)

			var best *MatchableRecord
			var bestScore float64
			var bestFieldScores map[string]float64
			for j := range candidates {
				tgt := candidates[j]
				if matchedTarget[tgt.ID] {
					continue
				}
				score, fieldScores, err := scoreCandidate(src, tgt, rule, registry)
				if err != nil {
					return outcome, err
				}
				if best == nil || score > bestScore {
					best = &candidates[j]
					bestScore = score
					bestFieldScores = fieldScores
				}
			}

			if best == nil || bestScore < rule.SuggestThresh {
				continue
			}

			cand := ScoredCandidate{
				RuleID: rule.ID, SourceIDs: []uuid.UUID{src.ID}, TargetIDs: []uuid.UUID{best.ID},
				Score: bestScore, FieldScores: bestFieldScores,
			}
			if bestScore >= rule.AutoMatchThresh {
				outcome.AutoMatched = append(outcome.AutoMatched, cand)
			} else {
				outcome.Suggested = append(outcome.Suggested, cand)
			}
			matchedSource[src.ID] = true
			matchedTarget[best.ID] = true
		}

		remainingSource = filterUnmatched(remainingSource, matchedSource)
		remainingTarget = filterUnmatched(remainingTarget, matchedTarget)
	}

	for _, s := range remainingSource {
		outcome.Unmatched = append(outcome.Unmatched, s.ID)
	}
	for _, t := range remainingTarget {
		outcome.Unmatched = append(outcome.Unmatched, t.ID)
	}

	return outcome, nil
}

func filterUnmatched(records []MatchableRecord, matched map[uuid.UUID]bool) []MatchableRecord {
	out := make([]MatchableRecord, 0, len(records))
	for _, r := range records {
		if !matched[r.ID] {
			out = append(out, r)
		}
	}
	return out
}
