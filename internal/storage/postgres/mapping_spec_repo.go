package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/koriebruh/Jengine/internal/domain"
)

// MappingSpecRepo implements domain.MappingSpecRepository.
type MappingSpecRepo struct{}

func NewMappingSpecRepo() *MappingSpecRepo {
	return &MappingSpecRepo{}
}

func (r *MappingSpecRepo) Create(ctx context.Context, tenantID uuid.UUID, m domain.MappingSpec) (domain.MappingSpec, error) {
	tx, err := requireTx(ctx, tenantID)
	if err != nil {
		return domain.MappingSpec{}, err
	}

	if m.ID == uuid.Nil {
		m.ID = uuid.New()
	}
	if m.Status == "" {
		m.Status = domain.MappingSpecStatusActive
	}
	err = tx.QueryRow(ctx,
		`INSERT INTO mapping_specs (id, tenant_id, source_format, version, status, spec)
		 VALUES ($1, $2, $3, $4, $5, $6)
		 RETURNING id, tenant_id, source_format, version, status, spec, created_at, updated_at`,
		m.ID, tenantID, m.SourceFormat, m.Version, m.Status, m.Spec,
	).Scan(&m.ID, &m.TenantID, &m.SourceFormat, &m.Version, &m.Status, &m.Spec, &m.CreatedAt, &m.UpdatedAt)
	if err != nil {
		return domain.MappingSpec{}, fmt.Errorf("postgres: MappingSpecRepo.Create: %w", err)
	}
	return m, nil
}

func (r *MappingSpecRepo) GetActive(ctx context.Context, tenantID uuid.UUID, sourceFormat string) (domain.MappingSpec, error) {
	tx, err := requireTx(ctx, tenantID)
	if err != nil {
		return domain.MappingSpec{}, err
	}

	var m domain.MappingSpec
	err = tx.QueryRow(ctx,
		`SELECT id, tenant_id, source_format, version, status, spec, created_at, updated_at
		 FROM mapping_specs WHERE source_format = $1 AND status = 'ACTIVE'
		 ORDER BY version DESC LIMIT 1`,
		sourceFormat,
	).Scan(&m.ID, &m.TenantID, &m.SourceFormat, &m.Version, &m.Status, &m.Spec, &m.CreatedAt, &m.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.MappingSpec{}, ErrNotFound
	}
	if err != nil {
		return domain.MappingSpec{}, fmt.Errorf("postgres: MappingSpecRepo.GetActive: %w", err)
	}
	return m, nil
}

func (r *MappingSpecRepo) UpdateStatus(ctx context.Context, tenantID uuid.UUID, id uuid.UUID, status domain.MappingSpecStatus) error {
	tx, err := requireTx(ctx, tenantID)
	if err != nil {
		return err
	}

	tag, err := tx.Exec(ctx, `UPDATE mapping_specs SET status = $1, updated_at = now() WHERE id = $2`, status, id)
	if err != nil {
		return fmt.Errorf("postgres: MappingSpecRepo.UpdateStatus: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

var _ domain.MappingSpecRepository = (*MappingSpecRepo)(nil)
