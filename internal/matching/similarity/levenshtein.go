package similarity

// LevenshteinNormalized returns 1 - (edit_distance / max(len(a), len(b))),
// i.e. 1.0 for identical strings, 0.0 for maximally different strings of
// the same length. Standard dynamic-programming edit distance -
// implemented here (not just left unregistered) because
// plans/docs/04-matching-engine.md §5.1's own worked example uses
// "levenshtein_normalized" for counterparty_ref; leaving it unregistered
// would make that exact example fail to compile, which is the primary
// manual-verification target for this task.
func LevenshteinNormalized(a, b string) float64 {
	r1, r2 := []rune(a), []rune(b)
	if len(r1) == 0 && len(r2) == 0 {
		return 1
	}
	maxLen := len(r1)
	if len(r2) > maxLen {
		maxLen = len(r2)
	}
	if maxLen == 0 {
		return 1
	}

	dist := levenshteinDistance(r1, r2)
	score := 1 - float64(dist)/float64(maxLen)
	if score < 0 {
		score = 0
	}
	return score
}

func levenshteinDistance(r1, r2 []rune) int {
	m, n := len(r1), len(r2)
	prev := make([]int, n+1)
	curr := make([]int, n+1)
	for j := 0; j <= n; j++ {
		prev[j] = j
	}

	for i := 1; i <= m; i++ {
		curr[0] = i
		for j := 1; j <= n; j++ {
			cost := 1
			if r1[i-1] == r2[j-1] {
				cost = 0
			}
			del := prev[j] + 1
			ins := curr[j-1] + 1
			sub := prev[j-1] + cost
			curr[j] = min(del, min(ins, sub))
		}
		prev, curr = curr, prev
	}
	return prev[n]
}
