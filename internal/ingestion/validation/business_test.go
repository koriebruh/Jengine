package validation_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/google/uuid"

	"github.com/koriebruh/Jengine/internal/ingestion/pipeline"
	"github.com/koriebruh/Jengine/internal/ingestion/validation"
)

func TestBusinessValidator_AmountSignRule(t *testing.T) {
	rule, _ := json.Marshal(map[string]any{"rule": "amount_sign", "side": "DEBIT", "allow_negative": false})
	v := &validation.BusinessValidator{Rules: []json.RawMessage{rule}}

	t.Run("negative debit violates disallow-negative policy", func(t *testing.T) {
		fields := validNormalizedFields() // Side=DEBIT, Amount=-100.00
		errs := v.Validate(context.Background(), validation.BusinessRuleContext{}, fields)
		if !hasFieldError(errs, "amount_sign") {
			t.Fatalf("expected an amount_sign violation, got %+v", errs)
		}
	})

	t.Run("positive debit satisfies disallow-negative policy", func(t *testing.T) {
		fields := validNormalizedFields()
		fields.Amount = fields.Amount.Neg() // now positive
		errs := v.Validate(context.Background(), validation.BusinessRuleContext{}, fields)
		if hasFieldError(errs, "amount_sign") {
			t.Fatalf("expected no amount_sign violation for a positive debit, got %+v", errs)
		}
	})

	t.Run("rule inapplicable to a different side", func(t *testing.T) {
		fields := validNormalizedFields()
		fields.Side = "CREDIT"
		errs := v.Validate(context.Background(), validation.BusinessRuleContext{}, fields)
		if hasFieldError(errs, "amount_sign") {
			t.Fatalf("expected the DEBIT-scoped rule to not apply to a CREDIT record, got %+v", errs)
		}
	})
}

func TestBusinessValidator_AccountAllowlistRule(t *testing.T) {
	allowedID := uuid.New()
	rule, _ := json.Marshal(map[string]any{"rule": "account_allowlist", "allowed_account_ids": []uuid.UUID{allowedID}})
	v := &validation.BusinessValidator{Rules: []json.RawMessage{rule}}

	t.Run("allowed account passes", func(t *testing.T) {
		errs := v.Validate(context.Background(), validation.BusinessRuleContext{AccountID: allowedID}, validNormalizedFields())
		if hasFieldError(errs, "account_allowlist") {
			t.Fatalf("expected no violation for an allowlisted account, got %+v", errs)
		}
	})

	t.Run("non-allowlisted account is rejected", func(t *testing.T) {
		errs := v.Validate(context.Background(), validation.BusinessRuleContext{AccountID: uuid.New()}, validNormalizedFields())
		if !hasFieldError(errs, "account_allowlist") {
			t.Fatalf("expected an account_allowlist violation naming the rule, got %+v", errs)
		}
	})
}

func TestBusinessValidator_UnknownRuleReportsError(t *testing.T) {
	rule, _ := json.Marshal(map[string]any{"rule": "does_not_exist"})
	v := &validation.BusinessValidator{Rules: []json.RawMessage{rule}}

	errs := v.Validate(context.Background(), validation.BusinessRuleContext{}, validNormalizedFields())
	if !hasFieldError(errs, "business_rule") {
		t.Fatalf("expected an unknown-rule error, got %+v", errs)
	}
}

func TestBusinessValidator_Register_CustomRule(t *testing.T) {
	validation.Register("always_fails_test_rule", func(ctx context.Context, rc validation.BusinessRuleContext, fields pipeline.NormalizedFields, raw json.RawMessage) error {
		return nil
	})
	if _, ok := validation.Get("always_fails_test_rule"); !ok {
		t.Fatal("expected the custom rule to be registered")
	}
}
