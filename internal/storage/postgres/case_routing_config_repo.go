package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/koriebruh/Jengine/internal/domain"
)

// CaseRoutingConfigRepo implements domain.CaseRoutingConfigRepository
// (plans/task/core/20).
type CaseRoutingConfigRepo struct{}

func NewCaseRoutingConfigRepo() *CaseRoutingConfigRepo { return &CaseRoutingConfigRepo{} }

func (r *CaseRoutingConfigRepo) GetActive(ctx context.Context, tenantID uuid.UUID) (domain.CaseRoutingConfig, error) {
	tx, err := requireTx(ctx, tenantID)
	if err != nil {
		return domain.CaseRoutingConfig{}, err
	}

	var c domain.CaseRoutingConfig
	err = tx.QueryRow(ctx,
		`SELECT id, tenant_id, version, status, config, created_by, created_at
		 FROM case_routing_configs WHERE status = 'ACTIVE' ORDER BY version DESC LIMIT 1`,
	).Scan(&c.ID, &c.TenantID, &c.Version, &c.Status, &c.Config, &c.CreatedBy, &c.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.CaseRoutingConfig{}, ErrNotFound
	}
	if err != nil {
		return domain.CaseRoutingConfig{}, fmt.Errorf("postgres: CaseRoutingConfigRepo.GetActive: %w", err)
	}
	return c, nil
}

func (r *CaseRoutingConfigRepo) Create(ctx context.Context, tenantID uuid.UUID, c domain.CaseRoutingConfig) (domain.CaseRoutingConfig, error) {
	tx, err := requireTx(ctx, tenantID)
	if err != nil {
		return domain.CaseRoutingConfig{}, err
	}

	err = tx.QueryRow(ctx,
		`INSERT INTO case_routing_configs (tenant_id, version, status, config, created_by)
		 VALUES ($1, $2, $3, $4, $5)
		 RETURNING id, created_at`,
		tenantID, c.Version, c.Status, c.Config, c.CreatedBy,
	).Scan(&c.ID, &c.CreatedAt)
	if err != nil {
		return domain.CaseRoutingConfig{}, fmt.Errorf("postgres: CaseRoutingConfigRepo.Create: %w", err)
	}
	c.TenantID = tenantID
	return c, nil
}

var _ domain.CaseRoutingConfigRepository = (*CaseRoutingConfigRepo)(nil)
