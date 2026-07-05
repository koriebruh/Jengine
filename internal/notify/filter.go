package notify

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
)

// MatchesFilter evaluates a subscription's simple FilterExpr (e.g.
// "amount_at_risk > 50000") against payload, a JSON event body -
// plans/task/core/21's own data model comment gives this exact example.
// Deliberately NOT a general expression language (no AND/OR, no
// arbitrary code execution) - one "field OP value" comparison, which is
// what the design doc's own example needs and all MVP scope calls for;
// building a fuller DSL here would be unscoped speculative work. An
// empty filterExpr always matches (no filtering configured).
func MatchesFilter(filterExpr string, payload []byte) (bool, error) {
	filterExpr = strings.TrimSpace(filterExpr)
	if filterExpr == "" {
		return true, nil
	}

	field, op, rawValue, err := parseFilterExpr(filterExpr)
	if err != nil {
		return false, err
	}

	var decoded map[string]any
	if err := json.Unmarshal(payload, &decoded); err != nil {
		return false, fmt.Errorf("notify: unmarshal payload for filter matching: %w", err)
	}
	actual, ok := decoded[field]
	if !ok {
		// Field absent from this particular event's payload - doesn't
		// match a numeric/string comparison, but isn't a filter-syntax
		// error either.
		return false, nil
	}

	return compare(actual, op, rawValue)
}

var filterOps = []string{">=", "<=", "!=", "==", ">", "<"} // longer operators first so ">=" isn't mis-split as ">"

func parseFilterExpr(expr string) (field, op, value string, err error) {
	for _, candidate := range filterOps {
		if idx := strings.Index(expr, candidate); idx >= 0 {
			field = strings.TrimSpace(expr[:idx])
			value = strings.TrimSpace(expr[idx+len(candidate):])
			return field, candidate, value, nil
		}
	}
	return "", "", "", fmt.Errorf("notify: unrecognized filter expression %q (expected \"field OP value\")", expr)
}

func compare(actual any, op string, rawValue string) (bool, error) {
	// Try numeric comparison first (the design's own example is
	// numeric: "amount_at_risk > 50000") - fall back to string equality
	// for ==/!= if either side isn't a valid number.
	actualNum, actualIsNum := toFloat(actual)
	valueNum, valueErr := strconv.ParseFloat(rawValue, 64)

	if actualIsNum && valueErr == nil {
		switch op {
		case ">":
			return actualNum > valueNum, nil
		case "<":
			return actualNum < valueNum, nil
		case ">=":
			return actualNum >= valueNum, nil
		case "<=":
			return actualNum <= valueNum, nil
		case "==":
			return actualNum == valueNum, nil
		case "!=":
			return actualNum != valueNum, nil
		}
	}

	actualStr := fmt.Sprintf("%v", actual)
	unquoted := strings.Trim(rawValue, `"'`)
	switch op {
	case "==":
		return actualStr == unquoted, nil
	case "!=":
		return actualStr != unquoted, nil
	default:
		return false, fmt.Errorf("notify: operator %q requires a numeric comparison, but field value %v isn't numeric", op, actual)
	}
}

func toFloat(v any) (float64, bool) {
	switch n := v.(type) {
	case float64:
		return n, true
	case int:
		return float64(n), true
	case string:
		f, err := strconv.ParseFloat(n, 64)
		return f, err == nil
	default:
		return 0, false
	}
}
