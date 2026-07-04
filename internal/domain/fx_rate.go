package domain

import (
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
)

// FXRate mirrors the fx_rates table - a static rate-table lookup for
// base-currency normalization (plans/docs/03-canonical-data-model.md
// §4.2's simpler MVP alternative to a live FX-provider connector). One
// current rate per (tenant, from, to) currency pair.
type FXRate struct {
	ID            uuid.UUID
	TenantID      uuid.UUID
	FromCurrency  string
	ToCurrency    string
	Rate          decimal.Decimal
	EffectiveDate time.Time
	CreatedAt     time.Time
	UpdatedAt     time.Time
}
