package domain_test

import (
	"errors"
	"testing"
	"time"

	"github.com/shopspring/decimal"

	"github.com/koriebruh/Jengine/internal/domain"
)

func TestMoney_Add_SameCurrency(t *testing.T) {
	a := domain.Money{Amount: decimal.RequireFromString("100.1234"), Currency: "USD"}
	b := domain.Money{Amount: decimal.RequireFromString("0.0001"), Currency: "USD"}

	got, err := a.Add(b)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := decimal.RequireFromString("100.1235")
	if !got.Amount.Equal(want) {
		t.Errorf("got %s, want %s", got.Amount, want)
	}
}

func TestMoney_Add_CurrencyMismatch(t *testing.T) {
	a := domain.Money{Amount: decimal.RequireFromString("100"), Currency: "USD"}
	b := domain.Money{Amount: decimal.RequireFromString("100"), Currency: "EUR"}

	_, err := a.Add(b)
	if !errors.Is(err, domain.ErrCurrencyMismatch) {
		t.Fatalf("expected ErrCurrencyMismatch, got %v", err)
	}
}

// TestMoney_NoFloatPrecisionLoss proves amounts survive round-trips
// exactly, unlike float64. 0.1 is the textbook case: it has no exact
// binary floating-point representation, so float64(0.1) != 0.1
// mathematically once you inspect enough digits - decimal.Decimal
// parses and stores it exactly as given, because it's a base-10
// fixed-point type, not binary floating point.
func TestMoney_NoFloatPrecisionLoss(t *testing.T) {
	m := domain.Money{Amount: decimal.RequireFromString("0.1"), Currency: "USD"}

	// The classic float precision failure: 0.1 + 0.2 != 0.3 in float64
	// arithmetic (it comes out to 0.30000000000000004). Prove the
	// decimal path does not have this problem.
	other := domain.Money{Amount: decimal.RequireFromString("0.2"), Currency: "USD"}
	sum, err := m.Add(other)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if sum.Amount.String() != "0.3" {
		t.Errorf("expected exact decimal 0.3, got %s (this is the float64 0.1+0.2 bug if it fails)", sum.Amount.String())
	}

	// A value with more precision than float64's ~15-17 significant
	// decimal digits can represent exactly - decimal.Decimal preserves
	// it verbatim as given.
	precise := decimal.RequireFromString("123456789012.123456789012")
	if precise.String() != "123456789012.123456789012" {
		t.Errorf("decimal value did not round-trip exactly: got %s", precise.String())
	}
}

func TestConvert(t *testing.T) {
	m := domain.Money{Amount: decimal.RequireFromString("100.00"), Currency: "EUR"}
	rate := decimal.RequireFromString("1.0850")
	rateDate := time.Date(2026, 1, 15, 0, 0, 0, 0, time.UTC)

	fx := domain.Convert(m, rate, rateDate)

	wantBase := decimal.RequireFromString("108.500000")
	if !fx.BaseAmount.Equal(wantBase) {
		t.Errorf("got base amount %s, want %s", fx.BaseAmount, wantBase)
	}
	if !fx.RateToBase.Equal(rate) {
		t.Errorf("got rate %s, want %s", fx.RateToBase, rate)
	}
	if !fx.RateDate.Equal(rateDate) {
		t.Errorf("got rate date %v, want %v", fx.RateDate, rateDate)
	}
}

func TestConvert_SameCurrencyIsRateOne(t *testing.T) {
	m := domain.Money{Amount: decimal.RequireFromString("50.00"), Currency: "USD"}
	fx := domain.Convert(m, decimal.NewFromInt(1), time.Now())

	if !fx.BaseAmount.Equal(m.Amount) {
		t.Errorf("expected same-currency conversion to preserve amount exactly, got %s want %s", fx.BaseAmount, m.Amount)
	}
}
