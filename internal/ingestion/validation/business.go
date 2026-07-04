package validation

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/google/uuid"

	"github.com/koriebruh/Jengine/internal/ingestion/pipeline"
)

// BusinessRuleContext carries per-run context a business rule needs
// beyond the record itself - AccountID is bound per pipeline run (same
// pattern as mapping.NormalizationStage.AccountID), since
// NormalizedFields doesn't carry it.
type BusinessRuleContext struct {
	AccountID uuid.UUID
}

// BusinessRuleFunc evaluates one configured rule against a record - rawRule
// is the rule's own JSON config (including its "rule" discriminator
// field), so each rule function parses whatever extra fields it needs.
// Returns a non-nil error (used as the ValidationError reason) if the
// rule is violated, nil if satisfied or inapplicable.
type BusinessRuleFunc func(ctx context.Context, rc BusinessRuleContext, fields pipeline.NormalizedFields, rawRule json.RawMessage) error

var (
	registryMu sync.RWMutex
	registry   = map[string]BusinessRuleFunc{
		"amount_sign":       amountSignRule,
		"account_allowlist": accountAllowlistRule,
	}
)

// Register adds or replaces a named business rule - kept deliberately
// narrow to the two rules plans/docs/02-data-ingestion.md §3.3 names,
// plus this extensibility point; not a general rule engine
// (plans/task/core/09 Non-Goals - that complexity belongs to the
// Matching Rule DSL, task 11, a different system for a different
// purpose).
func Register(name string, fn BusinessRuleFunc) {
	registryMu.Lock()
	defer registryMu.Unlock()
	registry[name] = fn
}

func Get(name string) (BusinessRuleFunc, bool) {
	registryMu.RLock()
	defer registryMu.RUnlock()
	fn, ok := registry[name]
	return fn, ok
}

type ruleHeader struct {
	Rule string `json:"rule"`
}

// BusinessValidator runs a tenant's configured set of business rules
// (raw JSON configs, e.g. loaded from tenant_settings) against a record.
type BusinessValidator struct {
	Rules []json.RawMessage
}

func (v *BusinessValidator) Validate(ctx context.Context, rc BusinessRuleContext, fields pipeline.NormalizedFields) []ValidationError {
	var errs []ValidationError
	for _, raw := range v.Rules {
		var h ruleHeader
		if err := json.Unmarshal(raw, &h); err != nil {
			errs = append(errs, ValidationError{Field: "business_rule", Reason: fmt.Sprintf("invalid rule config: %v", err)})
			continue
		}
		fn, ok := Get(h.Rule)
		if !ok {
			errs = append(errs, ValidationError{Field: "business_rule", Reason: fmt.Sprintf("unknown rule %q", h.Rule)})
			continue
		}
		if err := fn(ctx, rc, fields, raw); err != nil {
			errs = append(errs, ValidationError{Field: h.Rule, Reason: err.Error()})
		}
	}
	return errs
}

// amountSignParams: {"rule": "amount_sign", "side": "DEBIT", "allow_negative": false}
type amountSignParams struct {
	Side          string `json:"side"`
	AllowNegative bool   `json:"allow_negative"`
}

// amountSignRule checks side-vs-sign consistency per tenant policy: when
// the record's side matches the rule's configured side and
// allow_negative is false, the amount must not be negative. Rules that
// don't match the record's side are inapplicable (nil, not a failure).
func amountSignRule(ctx context.Context, rc BusinessRuleContext, fields pipeline.NormalizedFields, rawRule json.RawMessage) error {
	var p amountSignParams
	if err := json.Unmarshal(rawRule, &p); err != nil {
		return fmt.Errorf("amount_sign: invalid rule params: %w", err)
	}
	if string(fields.Side) != p.Side {
		return nil
	}
	if !p.AllowNegative && fields.Amount.IsNegative() {
		return fmt.Errorf("amount_sign: %s-side transaction has a negative amount %s, but this tenant's policy disallows it", p.Side, fields.Amount)
	}
	return nil
}

// accountAllowlistParams: {"rule": "account_allowlist", "allowed_account_ids": [...]}
type accountAllowlistParams struct {
	AllowedAccountIDs []uuid.UUID `json:"allowed_account_ids"`
}

func accountAllowlistRule(ctx context.Context, rc BusinessRuleContext, fields pipeline.NormalizedFields, rawRule json.RawMessage) error {
	var p accountAllowlistParams
	if err := json.Unmarshal(rawRule, &p); err != nil {
		return fmt.Errorf("account_allowlist: invalid rule params: %w", err)
	}
	for _, id := range p.AllowedAccountIDs {
		if id == rc.AccountID {
			return nil
		}
	}
	return fmt.Errorf("account_allowlist: account %s is not in the allowed list", rc.AccountID)
}
