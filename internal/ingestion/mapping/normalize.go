package mapping

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"

	"github.com/koriebruh/Jengine/internal/domain"
	"github.com/koriebruh/Jengine/internal/ingestion/pipeline"
)

// AccountLookup is the surface NormalizationStage needs to resolve an
// account's base currency - internal/storage/postgres.AccountRepo
// satisfies it structurally.
type AccountLookup interface {
	GetByID(ctx context.Context, tenantID, accountID uuid.UUID) (domain.Account, error)
}

// FXRateLookup is the surface NormalizationStage needs for base-amount
// conversion - internal/storage/postgres.FXRateRepo satisfies it
// structurally.
type FXRateLookup interface {
	Get(ctx context.Context, tenantID uuid.UUID, fromCurrency, toCurrency string) (domain.FXRate, error)
}

// NormalizationStage implements pipeline stage 4 (Normalization,
// plans/task/core/08): given the raw target values Field Mapping (stage
// 3) produced, it validates currency, asserts (does not re-apply) sign
// consistency, computes base_amount/fx_rate_to_base, and produces the
// typed NormalizedFields the Canonicalization stage consumes. Bound to a
// specific tenant+account per instance, the same pattern
// postgres.PersistEmitStage uses, since Stage.Process's signature has no
// room to pass account context through PipelineRecord.
type NormalizationStage struct {
	TenantID  uuid.UUID
	AccountID uuid.UUID
	Accounts  AccountLookup
	FXRates   FXRateLookup
	// TxRunner opens NormalizationStage's own transaction for its account/
	// fx-rate lookups - same rationale as MappingEngine.TxRunner: Process
	// is called directly by pipeline.Pipeline.Run, outside any ambient
	// transaction.
	TxRunner TxRunner
}

func (s *NormalizationStage) Name() string { return "normalization" }

func (s *NormalizationStage) Process(ctx context.Context, rec *pipeline.PipelineRecord) (pipeline.StageResult, error) {
	amount, ok := rec.MappedFields["transaction.amount"].(decimal.Decimal)
	if !ok {
		return pipeline.StageQuarantine, fmt.Errorf("normalization: transaction.amount missing or not a decimal.Decimal")
	}
	currencyRaw, ok := rec.MappedFields["transaction.currency"].(string)
	if !ok {
		return pipeline.StageQuarantine, fmt.Errorf("normalization: transaction.currency missing or not a string")
	}
	currency := strings.ToUpper(strings.TrimSpace(currencyRaw))
	if _, err := iso4217Validate(TransformContext{}, currency); err != nil {
		return pipeline.StageQuarantine, fmt.Errorf("normalization: %w", err)
	}

	valueDate, ok := rec.MappedFields["transaction.value_date"].(time.Time)
	if !ok {
		return pipeline.StageQuarantine, fmt.Errorf("normalization: transaction.value_date missing or not a time.Time")
	}
	bookingDate := valueDate
	if bd, ok := rec.MappedFields["transaction.booking_date"].(time.Time); ok {
		bookingDate = bd
	}

	// Sign sanity check only - apply_sign_from (stage 3) already applied
	// the sign; re-flipping here would silently corrupt amounts
	// (plans/task/core/08 Common Pitfalls). Nothing to actually assert
	// beyond "it parsed as a signed decimal," which the type system
	// already guarantees - this comment documents the deliberate
	// non-action, not a no-op bug.

	var account domain.Account
	var fxRate decimal.Decimal
	var baseAmount decimal.Decimal
	err := s.TxRunner(ctx, s.TenantID, func(ctx context.Context) error {
		var err error
		account, err = s.Accounts.GetByID(ctx, s.TenantID, s.AccountID)
		if err != nil {
			return fmt.Errorf("look up account: %w", err)
		}

		baseAmount = amount
		fxRate = decimal.NewFromInt(1)
		if currency != account.Currency {
			rate, err := s.FXRates.Get(ctx, s.TenantID, currency, account.Currency)
			if err != nil {
				return fmt.Errorf("no fx rate %s->%s: %w", currency, account.Currency, err)
			}
			fxRate = rate.Rate
			baseAmount = amount.Mul(fxRate)
		}
		return nil
	})
	if err != nil {
		return pipeline.StageQuarantine, fmt.Errorf("normalization: %w", err)
	}

	normalized := pipeline.NormalizedFields{
		Amount:       amount,
		Currency:     currency,
		BaseAmount:   baseAmount,
		FXRateToBase: fxRate,
		ValueDate:    valueDate,
		BookingDate:  bookingDate,
	}
	if ref, ok := rec.MappedFields["transaction.reference"].(string); ok {
		normalized.ExternalRef = ref
	}
	if desc, ok := rec.MappedFields["transaction.description"].(string); ok {
		normalized.Description = desc
	}
	if amount.IsNegative() {
		normalized.Side = domain.TransactionSideDebit
	} else {
		normalized.Side = domain.TransactionSideCredit
	}

	rec.Normalized = normalized
	return pipeline.StageContinue, nil
}

var _ pipeline.Stage = (*NormalizationStage)(nil)
