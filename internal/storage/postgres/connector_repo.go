package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/koriebruh/Jengine/internal/domain"
)

type ConnectorRepo struct{}

func NewConnectorRepo() *ConnectorRepo {
	return &ConnectorRepo{}
}

func (r *ConnectorRepo) Create(ctx context.Context, tenantID uuid.UUID, c domain.Connector) (domain.Connector, error) {
	tx, err := requireTx(ctx, tenantID)
	if err != nil {
		return domain.Connector{}, err
	}

	if c.ID == uuid.Nil {
		c.ID = uuid.New()
	}
	err = tx.QueryRow(ctx,
		`INSERT INTO connectors (id, tenant_id, type, config, schedule, status, last_run_at, cursor_state)
		 VALUES ($1, $2, $3, COALESCE($4, '{}'::jsonb), $5, $6, $7, $8)
		 RETURNING id, tenant_id, type, config, schedule, status, last_run_at, cursor_state, created_at, updated_at`,
		c.ID, tenantID, c.Type, nullableJSON(c.Config), c.Schedule, c.Status, c.LastRunAt, nullableJSON(c.CursorState),
	).Scan(&c.ID, &c.TenantID, &c.Type, &c.Config, &c.Schedule, &c.Status, &c.LastRunAt, &c.CursorState, &c.CreatedAt, &c.UpdatedAt)
	if err != nil {
		return domain.Connector{}, fmt.Errorf("postgres: ConnectorRepo.Create: %w", err)
	}
	return c, nil
}

func (r *ConnectorRepo) GetByID(ctx context.Context, tenantID uuid.UUID, id uuid.UUID) (domain.Connector, error) {
	tx, err := requireTx(ctx, tenantID)
	if err != nil {
		return domain.Connector{}, err
	}

	var c domain.Connector
	err = tx.QueryRow(ctx,
		`SELECT id, tenant_id, type, config, schedule, status, last_run_at, cursor_state, created_at, updated_at
		 FROM connectors WHERE id = $1`,
		id,
	).Scan(&c.ID, &c.TenantID, &c.Type, &c.Config, &c.Schedule, &c.Status, &c.LastRunAt, &c.CursorState, &c.CreatedAt, &c.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.Connector{}, ErrNotFound
	}
	if err != nil {
		return domain.Connector{}, fmt.Errorf("postgres: ConnectorRepo.GetByID: %w", err)
	}
	return c, nil
}

func (r *ConnectorRepo) ListByTenant(ctx context.Context, tenantID uuid.UUID) ([]domain.Connector, error) {
	tx, err := requireTx(ctx, tenantID)
	if err != nil {
		return nil, err
	}

	rows, err := tx.Query(ctx,
		`SELECT id, tenant_id, type, config, schedule, status, last_run_at, cursor_state, created_at, updated_at
		 FROM connectors ORDER BY created_at`,
	)
	if err != nil {
		return nil, fmt.Errorf("postgres: ConnectorRepo.ListByTenant: %w", err)
	}
	defer rows.Close()

	var connectors []domain.Connector
	for rows.Next() {
		var c domain.Connector
		if err := rows.Scan(&c.ID, &c.TenantID, &c.Type, &c.Config, &c.Schedule, &c.Status, &c.LastRunAt, &c.CursorState, &c.CreatedAt, &c.UpdatedAt); err != nil {
			return nil, fmt.Errorf("postgres: ConnectorRepo.ListByTenant: scan: %w", err)
		}
		connectors = append(connectors, c)
	}
	return connectors, rows.Err()
}

func (r *ConnectorRepo) UpdateCursorState(ctx context.Context, tenantID uuid.UUID, id uuid.UUID, cursorState []byte, lastRunAt time.Time) error {
	tx, err := requireTx(ctx, tenantID)
	if err != nil {
		return err
	}

	tag, err := tx.Exec(ctx,
		`UPDATE connectors SET cursor_state = COALESCE($1, cursor_state), last_run_at = $2, updated_at = now() WHERE id = $3`,
		nullableJSON(cursorState), lastRunAt, id,
	)
	if err != nil {
		return fmt.Errorf("postgres: ConnectorRepo.UpdateCursorState: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

var _ domain.ConnectorRepository = (*ConnectorRepo)(nil)
