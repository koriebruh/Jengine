package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/koriebruh/Jengine/internal/domain"
)

// FXRateRepo implements domain.FXRateRepository.
type FXRateRepo struct{}

func NewFXRateRepo() *FXRateRepo {
	return &FXRateRepo{}
}

func (r *FXRateRepo) Upsert(ctx context.Context, tenantID uuid.UUID, rate domain.FXRate) (domain.FXRate, error) {
	tx, err := requireTx(ctx, tenantID)
	if err != nil {
		return domain.FXRate{}, err
	}

	if rate.ID == uuid.Nil {
		rate.ID = uuid.New()
	}
	err = tx.QueryRow(ctx,
		`INSERT INTO fx_rates (id, tenant_id, from_currency, to_currency, rate, effective_date)
		 VALUES ($1, $2, $3, $4, $5, $6)
		 ON CONFLICT (tenant_id, from_currency, to_currency)
		 DO UPDATE SET rate = EXCLUDED.rate, effective_date = EXCLUDED.effective_date, updated_at = now()
		 RETURNING id, tenant_id, from_currency, to_currency, rate, effective_date, created_at, updated_at`,
		rate.ID, tenantID, rate.FromCurrency, rate.ToCurrency, rate.Rate, rate.EffectiveDate,
	).Scan(&rate.ID, &rate.TenantID, &rate.FromCurrency, &rate.ToCurrency, &rate.Rate, &rate.EffectiveDate, &rate.CreatedAt, &rate.UpdatedAt)
	if err != nil {
		return domain.FXRate{}, fmt.Errorf("postgres: FXRateRepo.Upsert: %w", err)
	}
	return rate, nil
}

func (r *FXRateRepo) Get(ctx context.Context, tenantID uuid.UUID, fromCurrency, toCurrency string) (domain.FXRate, error) {
	tx, err := requireTx(ctx, tenantID)
	if err != nil {
		return domain.FXRate{}, err
	}

	var rate domain.FXRate
	err = tx.QueryRow(ctx,
		`SELECT id, tenant_id, from_currency, to_currency, rate, effective_date, created_at, updated_at
		 FROM fx_rates WHERE from_currency = $1 AND to_currency = $2`,
		fromCurrency, toCurrency,
	).Scan(&rate.ID, &rate.TenantID, &rate.FromCurrency, &rate.ToCurrency, &rate.Rate, &rate.EffectiveDate, &rate.CreatedAt, &rate.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.FXRate{}, ErrNotFound
	}
	if err != nil {
		return domain.FXRate{}, fmt.Errorf("postgres: FXRateRepo.Get: %w", err)
	}
	return rate, nil
}

var _ domain.FXRateRepository = (*FXRateRepo)(nil)
