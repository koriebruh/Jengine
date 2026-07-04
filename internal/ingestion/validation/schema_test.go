package validation_test

import (
	"testing"
	"time"

	"github.com/shopspring/decimal"

	"github.com/koriebruh/Jengine/internal/domain"
	"github.com/koriebruh/Jengine/internal/ingestion/pipeline"
	"github.com/koriebruh/Jengine/internal/ingestion/validation"
)

func validNormalizedFields() pipeline.NormalizedFields {
	return pipeline.NormalizedFields{
		Amount:    decimal.RequireFromString("-100.00"),
		Currency:  "EUR",
		ValueDate: time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC),
		Side:      domain.TransactionSideDebit,
	}
}

func TestValidateSchema_ValidRecordPasses(t *testing.T) {
	errs := validation.ValidateSchema(validNormalizedFields())
	if len(errs) != 0 {
		t.Fatalf("expected no errors, got %+v", errs)
	}
}

func TestValidateSchema_MissingCurrency(t *testing.T) {
	fields := validNormalizedFields()
	fields.Currency = ""
	errs := validation.ValidateSchema(fields)
	if !hasFieldError(errs, "currency") {
		t.Fatalf("expected an error naming currency, got %+v", errs)
	}
}

func TestValidateSchema_InvalidCurrencyCode(t *testing.T) {
	fields := validNormalizedFields()
	fields.Currency = "XXX"
	errs := validation.ValidateSchema(fields)
	if !hasFieldError(errs, "currency") {
		t.Fatalf("expected an error naming currency for an invalid code, got %+v", errs)
	}
}

func TestValidateSchema_MissingValueDate(t *testing.T) {
	fields := validNormalizedFields()
	fields.ValueDate = time.Time{}
	errs := validation.ValidateSchema(fields)
	if !hasFieldError(errs, "value_date") {
		t.Fatalf("expected an error naming value_date, got %+v", errs)
	}
}

func TestValidateSchema_MissingSide(t *testing.T) {
	fields := validNormalizedFields()
	fields.Side = ""
	errs := validation.ValidateSchema(fields)
	if !hasFieldError(errs, "side") {
		t.Fatalf("expected an error naming side, got %+v", errs)
	}
}

func hasFieldError(errs []validation.ValidationError, field string) bool {
	for _, e := range errs {
		if e.Field == field {
			return true
		}
	}
	return false
}
