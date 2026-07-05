package mapping_test

import (
	"context"
	"testing"
	"time"

	"github.com/shopspring/decimal"

	"github.com/koriebruh/Jengine/internal/ingestion/mapping"
)

type fakeTokenizer struct{ tokens map[string]string }

func (f *fakeTokenizer) Tokenize(ctx context.Context, tenantID, field, value string) (string, error) {
	token := "tok_" + field + "_" + value
	if f.tokens == nil {
		f.tokens = make(map[string]string)
	}
	f.tokens[token] = value
	return token, nil
}

func (f *fakeTokenizer) Detokenize(ctx context.Context, tenantID, token string) (string, error) {
	return f.tokens[token], nil
}

func mustTransform(t *testing.T, name string) mapping.TransformFunc {
	t.Helper()
	fn, ok := mapping.Get(name)
	if !ok {
		t.Fatalf("transform %q not registered", name)
	}
	return fn
}

func TestParseDecimal(t *testing.T) {
	fn := mustTransform(t, "parse_decimal")

	t.Run("valid dot decimal", func(t *testing.T) {
		got, err := fn(mapping.TransformContext{}, "123.45")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !got.(decimal.Decimal).Equal(decimal.RequireFromString("123.45")) {
			t.Errorf("got %v", got)
		}
	})

	t.Run("MT940 comma decimal", func(t *testing.T) {
		got, err := fn(mapping.TransformContext{}, "250,00")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !got.(decimal.Decimal).Equal(decimal.RequireFromString("250.00")) {
			t.Errorf("got %v", got)
		}
	})

	t.Run("invalid input", func(t *testing.T) {
		if _, err := fn(mapping.TransformContext{}, "not-a-number"); err == nil {
			t.Fatal("expected an error for non-numeric input")
		}
	})

	t.Run("wrong type", func(t *testing.T) {
		if _, err := fn(mapping.TransformContext{}, 123); err == nil {
			t.Fatal("expected an error for non-string input")
		}
	})
}

func TestApplySignFrom(t *testing.T) {
	fn := mustTransform(t, "apply_sign_from")
	ctx := mapping.TransformContext{Record: map[string]any{
		"field_61": map[string]any{"debit_credit_mark": "D"},
	}}
	ctxCredit := mapping.TransformContext{Record: map[string]any{
		"field_61": map[string]any{"debit_credit_mark": "C"},
	}}

	t.Run("debit negates", func(t *testing.T) {
		got, err := fn(ctx, decimal.RequireFromString("100"), "field_61.debit_credit_mark")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !got.(decimal.Decimal).Equal(decimal.RequireFromString("-100")) {
			t.Errorf("got %v", got)
		}
	})

	t.Run("credit unchanged", func(t *testing.T) {
		got, err := fn(ctxCredit, decimal.RequireFromString("100"), "field_61.debit_credit_mark")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !got.(decimal.Decimal).Equal(decimal.RequireFromString("100")) {
			t.Errorf("got %v", got)
		}
	})

	t.Run("missing field errors", func(t *testing.T) {
		if _, err := fn(mapping.TransformContext{Record: map[string]any{}}, decimal.RequireFromString("100"), "field_61.debit_credit_mark"); err == nil {
			t.Fatal("expected an error for a missing sibling field")
		}
	})

	t.Run("non-decimal value errors", func(t *testing.T) {
		if _, err := fn(ctx, "100", "field_61.debit_credit_mark"); err == nil {
			t.Fatal("expected an error when value isn't already a decimal.Decimal")
		}
	})

	t.Run("unrecognized mark errors", func(t *testing.T) {
		badCtx := mapping.TransformContext{Record: map[string]any{"field_61": map[string]any{"debit_credit_mark": "X"}}}
		if _, err := fn(badCtx, decimal.RequireFromString("100"), "field_61.debit_credit_mark"); err == nil {
			t.Fatal("expected an error for an unrecognized debit/credit mark")
		}
	})
}

func TestUppercaseAndTrim(t *testing.T) {
	upper := mustTransform(t, "uppercase")
	got, err := upper(mapping.TransformContext{}, "eur")
	if err != nil || got != "EUR" {
		t.Errorf("uppercase(eur) = %v, %v", got, err)
	}

	trim := mustTransform(t, "trim")
	got, err = trim(mapping.TransformContext{}, "  hello  ")
	if err != nil || got != "hello" {
		t.Errorf("trim = %v, %v", got, err)
	}
}

func TestISO4217Validate(t *testing.T) {
	fn := mustTransform(t, "iso4217_validate")

	t.Run("valid code", func(t *testing.T) {
		got, err := fn(mapping.TransformContext{}, "eur")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got != "EUR" {
			t.Errorf("got %v", got)
		}
	})

	t.Run("invalid code", func(t *testing.T) {
		if _, err := fn(mapping.TransformContext{}, "XXX"); err == nil {
			t.Fatal("expected an error for an unrecognized currency code")
		}
	})
}

func TestParseDate(t *testing.T) {
	fn := mustTransform(t, "parse_date")

	t.Run("YYMMDD against a real 6-digit date", func(t *testing.T) {
		got, err := fn(mapping.TransformContext{}, "240102", "YYMMDD")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		tm := got.(time.Time)
		if tm.Year() != 2024 || tm.Month() != 1 || tm.Day() != 2 {
			t.Errorf("got %v", tm)
		}
	})

	t.Run("YYYY-MM-DD", func(t *testing.T) {
		got, err := fn(mapping.TransformContext{}, "2024-03-15", "YYYY-MM-DD")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		tm := got.(time.Time)
		if tm.Year() != 2024 || tm.Month() != 3 || tm.Day() != 15 {
			t.Errorf("got %v", tm)
		}
	})

	t.Run("mismatched layout errors", func(t *testing.T) {
		if _, err := fn(mapping.TransformContext{}, "not-a-date", "YYMMDD"); err == nil {
			t.Fatal("expected an error for a mismatched layout")
		}
	})

	t.Run("missing layout arg errors", func(t *testing.T) {
		if _, err := fn(mapping.TransformContext{}, "240102"); err == nil {
			t.Fatal("expected an error when no layout argument is given")
		}
	})
}

func TestExtractRegex(t *testing.T) {
	fn := mustTransform(t, "extract_regex")

	t.Run("REF pattern matches", func(t *testing.T) {
		got, err := fn(mapping.TransformContext{}, "some narrative REF:ABC123 trailing text", `REF:(\S+)`)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got != "ABC123" {
			t.Errorf("got %v", got)
		}
	})

	t.Run("no match errors", func(t *testing.T) {
		if _, err := fn(mapping.TransformContext{}, "no reference here", `REF:(\S+)`); err == nil {
			t.Fatal("expected an error when the pattern doesn't match")
		}
	})

	t.Run("invalid pattern errors", func(t *testing.T) {
		if _, err := fn(mapping.TransformContext{}, "text", `(unterminated`); err == nil {
			t.Fatal("expected an error for an invalid regex pattern")
		}
	})
}

func TestTokenize(t *testing.T) {
	fn := mustTransform(t, "tokenize")

	t.Run("replaces value with an opaque token", func(t *testing.T) {
		tokenizer := &fakeTokenizer{}
		ctx := mapping.TransformContext{
			Ctx: context.Background(), TenantID: "tenant-1", Tokenizer: tokenizer, TargetField: "transaction.raw_payload.card_number",
		}
		got, err := fn(ctx, "4111111111111111")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		token, ok := got.(string)
		if !ok || token == "4111111111111111" {
			t.Errorf("expected a token distinct from the raw value, got %v", got)
		}
	})

	t.Run("nil Tokenizer fails loudly rather than passing the value through", func(t *testing.T) {
		if _, err := fn(mapping.TransformContext{}, "4111111111111111"); err == nil {
			t.Fatal("expected an error when no Tokenizer is configured")
		}
	})

	t.Run("non-string value errors", func(t *testing.T) {
		ctx := mapping.TransformContext{Ctx: context.Background(), Tokenizer: &fakeTokenizer{}}
		if _, err := fn(ctx, 12345); err == nil {
			t.Fatal("expected an error for a non-string value")
		}
	})
}

func TestRegister_CustomTransform(t *testing.T) {
	mapping.Register("test_double", func(ctx mapping.TransformContext, value any, args ...string) (any, error) {
		d := value.(decimal.Decimal)
		return d.Mul(decimal.NewFromInt(2)), nil
	})

	fn := mustTransform(t, "test_double")
	got, err := fn(mapping.TransformContext{}, decimal.RequireFromString("21"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !got.(decimal.Decimal).Equal(decimal.RequireFromString("42")) {
		t.Errorf("got %v", got)
	}
}
