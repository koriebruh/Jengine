package mapping

import (
	"fmt"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/shopspring/decimal"
)

// TransformContext gives a transform function access to sibling fields on
// the record being mapped - needed by transforms like apply_sign_from
// that reference another field rather than operating purely on their own
// input value.
type TransformContext struct {
	Record map[string]any
}

// TransformFunc is one named, chainable transform step. args holds at
// most one element (this DSL's grammar is deliberately narrow - a single
// optional string/field-reference argument, not general expression
// evaluation - plans/task/core/08 Non-Goals).
type TransformFunc func(ctx TransformContext, value any, args ...string) (any, error)

var (
	registryMu sync.RWMutex
	registry   = map[string]TransformFunc{
		"parse_decimal":    parseDecimal,
		"apply_sign_from":  applySignFrom,
		"uppercase":        uppercase,
		"trim":             trim,
		"iso4217_validate": iso4217Validate,
		"parse_date":       parseDate,
		"extract_regex":    extractRegex,
	}
)

// Register adds or replaces a named transform - lets later tasks add
// format-specific transforms without modifying this file. Resist adding
// speculative transforms here beyond the explicit §3.2 list
// (plans/task/core/08 Implementation Notes).
func Register(name string, fn TransformFunc) {
	registryMu.Lock()
	defer registryMu.Unlock()
	registry[name] = fn
}

// Get looks up a registered transform by name.
func Get(name string) (TransformFunc, bool) {
	registryMu.RLock()
	defer registryMu.RUnlock()
	fn, ok := registry[name]
	return fn, ok
}

// parseDecimal converts a string amount to decimal.Decimal. MT940-style
// comma decimal separators (e.g. "250,00") are detected and normalized -
// if the string contains a comma but no dot, the comma is treated as the
// decimal point.
func parseDecimal(ctx TransformContext, value any, args ...string) (any, error) {
	s, ok := value.(string)
	if !ok {
		return nil, fmt.Errorf("parse_decimal: expected a string, got %T", value)
	}
	s = strings.TrimSpace(s)
	if strings.Contains(s, ",") && !strings.Contains(s, ".") {
		s = strings.Replace(s, ",", ".", 1)
	}
	d, err := decimal.NewFromString(s)
	if err != nil {
		return nil, fmt.Errorf("parse_decimal: %w", err)
	}
	return d, nil
}

// applySignFrom flips value's sign based on the sibling field named by
// args[0] (resolved via a dot-path lookup against ctx.Record), whose
// value is interpreted as a debit/credit mark: "D"/"RD" -> negative
// (debit reduces balance), "C"/"RC" -> unchanged (credit). value must
// already be a decimal.Decimal (i.e. parse_decimal must run earlier in
// the chain).
func applySignFrom(ctx TransformContext, value any, args ...string) (any, error) {
	if len(args) == 0 {
		return nil, fmt.Errorf("apply_sign_from: requires a field-reference argument")
	}
	d, ok := value.(decimal.Decimal)
	if !ok {
		return nil, fmt.Errorf("apply_sign_from: expected a decimal.Decimal (run parse_decimal first), got %T", value)
	}

	markVal, err := resolveFieldPath(ctx.Record, args[0])
	if err != nil {
		return nil, fmt.Errorf("apply_sign_from: %w", err)
	}
	mark, ok := markVal.(string)
	if !ok {
		return nil, fmt.Errorf("apply_sign_from: field %q is not a string (got %T)", args[0], markVal)
	}

	mark = strings.ToUpper(strings.TrimSpace(mark))
	if strings.HasPrefix(mark, "D") {
		return d.Neg(), nil
	}
	if strings.HasPrefix(mark, "C") {
		return d, nil
	}
	return nil, fmt.Errorf("apply_sign_from: unrecognized debit/credit mark %q (expected D/C/RD/RC)", mark)
}

func uppercase(ctx TransformContext, value any, args ...string) (any, error) {
	s, ok := value.(string)
	if !ok {
		return nil, fmt.Errorf("uppercase: expected a string, got %T", value)
	}
	return strings.ToUpper(s), nil
}

func trim(ctx TransformContext, value any, args ...string) (any, error) {
	s, ok := value.(string)
	if !ok {
		return nil, fmt.Errorf("trim: expected a string, got %T", value)
	}
	return strings.TrimSpace(s), nil
}

// iso4217Currencies is a non-exhaustive but broadly sufficient set of
// active ISO 4217 currency codes for MVP validation - not a complete
// registry (plans/task/core/08 Non-Goals: avoid over-building this).
var iso4217Currencies = map[string]bool{
	"USD": true, "EUR": true, "GBP": true, "JPY": true, "CHF": true,
	"AUD": true, "CAD": true, "NZD": true, "CNY": true, "HKD": true,
	"SGD": true, "SEK": true, "NOK": true, "DKK": true, "PLN": true,
	"CZK": true, "HUF": true, "ZAR": true, "MXN": true, "BRL": true,
	"INR": true, "KRW": true, "IDR": true, "THB": true, "MYR": true,
	"PHP": true, "VND": true, "AED": true, "SAR": true, "ILS": true,
	"TRY": true, "RUB": true,
}

func iso4217Validate(ctx TransformContext, value any, args ...string) (any, error) {
	s, ok := value.(string)
	if !ok {
		return nil, fmt.Errorf("iso4217_validate: expected a string, got %T", value)
	}
	if err := ValidateISO4217(s); err != nil {
		return nil, err
	}
	return strings.ToUpper(strings.TrimSpace(s)), nil
}

// ValidateISO4217 checks code against the same currency set the
// iso4217_validate transform uses - exported so other packages (e.g.
// plans/task/core/09's schema validation) reuse this instead of
// reimplementing currency validation (plans/task/core/09 Implementation
// Notes).
func ValidateISO4217(code string) error {
	s := strings.ToUpper(strings.TrimSpace(code))
	if !iso4217Currencies[s] {
		return fmt.Errorf("iso4217_validate: %q is not a recognized ISO 4217 currency code", s)
	}
	return nil
}

// dateTokenReplacer translates a tenant-facing token vocabulary
// (YYYY/MM/DD/HH/mm/ss) into Go's reference-time layout syntax, so
// non-Go-literate ops authors never need to know Go's "060102"-style
// layout strings (plans/task/core/08 Implementation Notes/Common
// Pitfalls). Longer tokens must be replaced before their substrings
// (YYYY before YY) - order matters here.
var dateTokenOrder = []struct{ token, goLayout string }{
	{"YYYY", "2006"},
	{"YY", "06"},
	{"MM", "01"},
	{"DD", "02"},
	{"HH", "15"},
	{"mm", "04"},
	{"ss", "05"},
}

func translateDateLayout(tenantLayout string) string {
	out := tenantLayout
	for _, tok := range dateTokenOrder {
		out = strings.ReplaceAll(out, tok.token, tok.goLayout)
	}
	return out
}

func parseDate(ctx TransformContext, value any, args ...string) (any, error) {
	if len(args) == 0 {
		return nil, fmt.Errorf("parse_date: requires a layout argument, e.g. parse_date(\"YYMMDD\")")
	}
	s, ok := value.(string)
	if !ok {
		return nil, fmt.Errorf("parse_date: expected a string, got %T", value)
	}
	goLayout := translateDateLayout(args[0])
	t, err := time.Parse(goLayout, s)
	if err != nil {
		return nil, fmt.Errorf("parse_date: %q does not match layout %q: %w", s, args[0], err)
	}
	return t, nil
}

func extractRegex(ctx TransformContext, value any, args ...string) (any, error) {
	if len(args) == 0 {
		return nil, fmt.Errorf("extract_regex: requires a pattern argument")
	}
	s, ok := value.(string)
	if !ok {
		return nil, fmt.Errorf("extract_regex: expected a string, got %T", value)
	}
	re, err := regexp.Compile(args[0])
	if err != nil {
		return nil, fmt.Errorf("extract_regex: invalid pattern %q: %w", args[0], err)
	}
	m := re.FindStringSubmatch(s)
	if len(m) < 2 {
		return nil, fmt.Errorf("extract_regex: pattern %q did not match (with a capture group) against %q", args[0], s)
	}
	return m[1], nil
}

// resolveFieldPath looks up a dot-separated path (e.g.
// "field_61.debit_credit_mark") in a nested map[string]any.
func resolveFieldPath(record map[string]any, path string) (any, error) {
	parts := strings.Split(path, ".")
	var cur any = record
	for i, part := range parts {
		m, ok := cur.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("field path %q: %q is not an object at segment %d", path, part, i)
		}
		v, ok := m[part]
		if !ok {
			return nil, fmt.Errorf("field path %q: no such field %q", path, part)
		}
		cur = v
	}
	return cur, nil
}
