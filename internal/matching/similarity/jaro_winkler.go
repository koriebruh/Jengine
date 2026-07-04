package similarity

// JaroWinkler returns the Jaro-Winkler similarity of s1 and s2 in
// [0,1] - standard algorithm, including the Winkler common-prefix boost
// (plans/task/core/11 Common Pitfalls: plain Jaro without the prefix
// adjustment is a different, weaker metric - this must include it).
// Used for short strings with typos near the start (references, names)
// per plans/docs/04-matching-engine.md §5.3.
func JaroWinkler(s1, s2 string) float64 {
	jaro := JaroSimilarity(s1, s2)
	if jaro == 0 {
		return 0
	}

	const (
		prefixScale  = 0.1
		maxPrefixLen = 4
	)

	r1, r2 := []rune(s1), []rune(s2)
	prefixLen := 0
	maxLen := len(r1)
	if len(r2) < maxLen {
		maxLen = len(r2)
	}
	if maxLen > maxPrefixLen {
		maxLen = maxPrefixLen
	}
	for i := 0; i < maxLen; i++ {
		if r1[i] != r2[i] {
			break
		}
		prefixLen++
	}

	return jaro + float64(prefixLen)*prefixScale*(1-jaro)
}

// JaroSimilarity returns the plain Jaro similarity (no Winkler prefix
// boost) of s1 and s2 in [0,1].
func JaroSimilarity(s1, s2 string) float64 {
	r1, r2 := []rune(s1), []rune(s2)
	len1, len2 := len(r1), len(r2)

	if len1 == 0 && len2 == 0 {
		return 1
	}
	if len1 == 0 || len2 == 0 {
		return 0
	}

	matchWindow := max(len1, len2)/2 - 1
	if matchWindow < 0 {
		matchWindow = 0
	}

	s1Matches := make([]bool, len1)
	s2Matches := make([]bool, len2)

	matches := 0
	for i := 0; i < len1; i++ {
		start := i - matchWindow
		if start < 0 {
			start = 0
		}
		end := i + matchWindow + 1
		if end > len2 {
			end = len2
		}
		for j := start; j < end; j++ {
			if s2Matches[j] || r1[i] != r2[j] {
				continue
			}
			s1Matches[i] = true
			s2Matches[j] = true
			matches++
			break
		}
	}

	if matches == 0 {
		return 0
	}

	transpositions := 0
	k := 0
	for i := 0; i < len1; i++ {
		if !s1Matches[i] {
			continue
		}
		for !s2Matches[k] {
			k++
		}
		if r1[i] != r2[k] {
			transpositions++
		}
		k++
	}
	transpositions /= 2

	m := float64(matches)
	return (m/float64(len1) + m/float64(len2) + (m-float64(transpositions))/m) / 3
}
