package domain

import (
	"fmt"
	"time"

	"github.com/shopspring/decimal"
)

// Money is the multi-currency value type used everywhere an amount
// appears. Deliberate choice: decimal.Decimal, never float64 - matches
// the numeric(20,4) Postgres column type exactly and avoids
// float-precision bugs in a financial system (plans/task/core/05
// Implementation Notes).
type Money struct {
	Amount   decimal.Decimal
	Currency string // ISO 4217, 3-char uppercase
}

// ErrCurrencyMismatch is returned by operations that require two Money
// values to share a currency (e.g. Add) when they don't.
var ErrCurrencyMismatch = fmt.Errorf("domain: currency mismatch")

// Add returns m+other, erroring if the currencies differ - callers must
// convert to a common currency (via FXConversion) before combining
// amounts across currencies; this type never does that implicitly.
func (m Money) Add(other Money) (Money, error) {
	if m.Currency != other.Currency {
		return Money{}, fmt.Errorf("%w: %s vs %s", ErrCurrencyMismatch, m.Currency, other.Currency)
	}
	return Money{Amount: m.Amount.Add(other.Amount), Currency: m.Currency}, nil
}

// FXConversion is the historical fact of converting a Money value to the
// tenant/account's base currency at ingestion time. RateToBase/RateDate
// are captured once and never recomputed later - plans/docs/03-canonical-data-model.md
// §4.2 treats them as an audit-relevant historical record, not a live
// lookup that could silently change a transaction's base_amount after
// the fact.
type FXConversion struct {
	BaseAmount decimal.Decimal
	RateToBase decimal.Decimal
	RateDate   time.Time
}

// Convert applies rate to m, returning the resulting FXConversion. Same
// currency (rate == 1) is still a valid call - it's how a same-currency
// Transaction's base_amount ends up trivially equal to its native amount
// without a special-cased code path.
func Convert(m Money, rateToBase decimal.Decimal, rateDate time.Time) FXConversion {
	return FXConversion{
		BaseAmount: m.Amount.Mul(rateToBase),
		RateToBase: rateToBase,
		RateDate:   rateDate,
	}
}
