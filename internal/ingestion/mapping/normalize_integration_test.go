package mapping_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"

	"github.com/koriebruh/Jengine/internal/ingestion/mapping"
	"github.com/koriebruh/Jengine/internal/ingestion/pipeline"
	"github.com/koriebruh/Jengine/internal/storage/postgres"
	"github.com/koriebruh/Jengine/internal/tenancy"
	"github.com/koriebruh/Jengine/internal/testutil"
)

// TestNormalizationStage_FXConversion proves plans/task/core/08's
// Definition of Done: same-currency transactions get rate=1/base_amount
// unchanged, and cross-currency transactions get correctly converted
// against a seeded fx_rates table entry.
func TestNormalizationStage_FXConversion(t *testing.T) {
	if testing.Short() {
		t.Skip("requires Docker - skipped under -short; run make test-integration")
	}

	db := testutil.StartPostgres(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	tenantID := uuid.New()
	if _, err := db.Pool.Exec(ctx,
		`INSERT INTO tenants (id, name, isolation_tier, region, status) VALUES ($1, 'Acme', 'STANDARD', 'us-east', 'ACTIVE')`,
		tenantID,
	); err != nil {
		t.Fatalf("seed tenant failed: %v", err)
	}
	accountID := uuid.New()
	if _, err := db.Pool.Exec(ctx,
		`INSERT INTO accounts (id, tenant_id, external_account_ref, account_type, currency, name) VALUES ($1, $2, 'ACC-1', 'BANK', 'USD', 'Base USD Account')`,
		accountID, tenantID,
	); err != nil {
		t.Fatalf("seed account failed: %v", err)
	}
	if _, err := db.Pool.Exec(ctx,
		`INSERT INTO fx_rates (tenant_id, from_currency, to_currency, rate, effective_date) VALUES ($1, 'EUR', 'USD', 1.0850, CURRENT_DATE)`,
		tenantID,
	); err != nil {
		t.Fatalf("seed fx rate failed: %v", err)
	}

	appPool := testutil.AppRolePool(t, ctx, db.DSN)
	defer appPool.Close()

	accountRepo := postgres.NewAccountRepo()
	fxRateRepo := postgres.NewFXRateRepo()

	txRunner := func(ctx context.Context, tid uuid.UUID, fn func(ctx context.Context) error) error {
		return postgres.WithTx(tenancy.WithTenant(ctx, tenancy.TenantContext{TenantID: tid}), appPool, tid, fn)
	}

	stage := &mapping.NormalizationStage{
		TenantID: tenantID, AccountID: accountID,
		Accounts: accountRepo, FXRates: fxRateRepo,
		TxRunner: txRunner,
	}

	t.Run("same currency: rate is 1, base_amount unchanged", func(t *testing.T) {
		rec := &pipeline.PipelineRecord{
			MappedFields: map[string]any{
				"transaction.amount":     decimal.RequireFromString("100.00"),
				"transaction.currency":   "USD",
				"transaction.value_date": time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC),
			},
		}
		result, err := stage.Process(ctx, rec)
		if err != nil {
			t.Fatalf("Process failed: %v", err)
		}
		if result != pipeline.StageContinue {
			t.Fatalf("expected StageContinue, got %v", result)
		}
		if !rec.Normalized.FXRateToBase.Equal(decimal.NewFromInt(1)) {
			t.Errorf("expected fx_rate_to_base 1, got %s", rec.Normalized.FXRateToBase)
		}
		if !rec.Normalized.BaseAmount.Equal(decimal.RequireFromString("100.00")) {
			t.Errorf("expected base_amount 100.00, got %s", rec.Normalized.BaseAmount)
		}
	})

	t.Run("cross currency: converts using the seeded fx_rates entry", func(t *testing.T) {
		rec := &pipeline.PipelineRecord{
			MappedFields: map[string]any{
				"transaction.amount":     decimal.RequireFromString("200.00"),
				"transaction.currency":   "EUR",
				"transaction.value_date": time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC),
			},
		}
		result, err := stage.Process(ctx, rec)
		if err != nil {
			t.Fatalf("Process failed: %v", err)
		}
		if result != pipeline.StageContinue {
			t.Fatalf("expected StageContinue, got %v", result)
		}
		if !rec.Normalized.FXRateToBase.Equal(decimal.RequireFromString("1.0850")) {
			t.Errorf("expected fx_rate_to_base 1.0850, got %s", rec.Normalized.FXRateToBase)
		}
		wantBase := decimal.RequireFromString("200.00").Mul(decimal.RequireFromString("1.0850"))
		if !rec.Normalized.BaseAmount.Equal(wantBase) {
			t.Errorf("expected base_amount %s, got %s", wantBase, rec.Normalized.BaseAmount)
		}
	})

	t.Run("missing fx rate quarantines", func(t *testing.T) {
		rec := &pipeline.PipelineRecord{
			MappedFields: map[string]any{
				"transaction.amount":     decimal.RequireFromString("50.00"),
				"transaction.currency":   "GBP", // no fx_rates entry seeded for GBP->USD
				"transaction.value_date": time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC),
			},
		}
		result, err := stage.Process(ctx, rec)
		if result != pipeline.StageQuarantine {
			t.Errorf("expected StageQuarantine for a missing fx rate, got %v (err=%v)", result, err)
		}
		if err == nil {
			t.Error("expected a non-nil error for a missing fx rate")
		}
	})
}
