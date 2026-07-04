package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/koriebruh/Jengine/internal/domain"
)

type StatementRepo struct{}

func NewStatementRepo() *StatementRepo {
	return &StatementRepo{}
}

func (r *StatementRepo) Create(ctx context.Context, tenantID uuid.UUID, s domain.Statement) (domain.Statement, error) {
	tx, err := requireTx(ctx, tenantID)
	if err != nil {
		return domain.Statement{}, err
	}

	if s.ID == uuid.Nil {
		s.ID = uuid.New()
	}
	err = tx.QueryRow(ctx,
		`INSERT INTO statements (id, tenant_id, account_id, source_connector_id, format, received_at, period_start, period_end, opening_balance, closing_balance, status, raw_file_ref, checksum)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
		 RETURNING id, tenant_id, account_id, source_connector_id, format, received_at, period_start, period_end, opening_balance, closing_balance, status, raw_file_ref, checksum, created_at, updated_at`,
		s.ID, tenantID, s.AccountID, s.SourceConnectorID, s.Format, s.ReceivedAt, s.PeriodStart, s.PeriodEnd, s.OpeningBalance, s.ClosingBalance, s.Status, s.RawFileRef, s.Checksum,
	).Scan(&s.ID, &s.TenantID, &s.AccountID, &s.SourceConnectorID, &s.Format, &s.ReceivedAt, &s.PeriodStart, &s.PeriodEnd, &s.OpeningBalance, &s.ClosingBalance, &s.Status, &s.RawFileRef, &s.Checksum, &s.CreatedAt, &s.UpdatedAt)
	if err != nil {
		return domain.Statement{}, fmt.Errorf("postgres: StatementRepo.Create: %w", err)
	}
	return s, nil
}

func (r *StatementRepo) GetByID(ctx context.Context, tenantID uuid.UUID, id uuid.UUID) (domain.Statement, error) {
	tx, err := requireTx(ctx, tenantID)
	if err != nil {
		return domain.Statement{}, err
	}

	var s domain.Statement
	err = tx.QueryRow(ctx,
		`SELECT id, tenant_id, account_id, source_connector_id, format, received_at, period_start, period_end, opening_balance, closing_balance, status, raw_file_ref, checksum, created_at, updated_at
		 FROM statements WHERE id = $1`,
		id,
	).Scan(&s.ID, &s.TenantID, &s.AccountID, &s.SourceConnectorID, &s.Format, &s.ReceivedAt, &s.PeriodStart, &s.PeriodEnd, &s.OpeningBalance, &s.ClosingBalance, &s.Status, &s.RawFileRef, &s.Checksum, &s.CreatedAt, &s.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.Statement{}, ErrNotFound
	}
	if err != nil {
		return domain.Statement{}, fmt.Errorf("postgres: StatementRepo.GetByID: %w", err)
	}
	return s, nil
}

func (r *StatementRepo) UpdateStatus(ctx context.Context, tenantID uuid.UUID, id uuid.UUID, status domain.StatementStatus) error {
	tx, err := requireTx(ctx, tenantID)
	if err != nil {
		return err
	}

	tag, err := tx.Exec(ctx,
		`UPDATE statements SET status = $1, updated_at = now() WHERE id = $2`,
		status, id,
	)
	if err != nil {
		return fmt.Errorf("postgres: StatementRepo.UpdateStatus: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

func (r *StatementRepo) ListByAccount(ctx context.Context, tenantID uuid.UUID, accountID uuid.UUID) ([]domain.Statement, error) {
	tx, err := requireTx(ctx, tenantID)
	if err != nil {
		return nil, err
	}

	rows, err := tx.Query(ctx,
		`SELECT id, tenant_id, account_id, source_connector_id, format, received_at, period_start, period_end, opening_balance, closing_balance, status, raw_file_ref, checksum, created_at, updated_at
		 FROM statements WHERE account_id = $1 ORDER BY received_at DESC`,
		accountID,
	)
	if err != nil {
		return nil, fmt.Errorf("postgres: StatementRepo.ListByAccount: %w", err)
	}
	defer rows.Close()

	var statements []domain.Statement
	for rows.Next() {
		var s domain.Statement
		if err := rows.Scan(&s.ID, &s.TenantID, &s.AccountID, &s.SourceConnectorID, &s.Format, &s.ReceivedAt, &s.PeriodStart, &s.PeriodEnd, &s.OpeningBalance, &s.ClosingBalance, &s.Status, &s.RawFileRef, &s.Checksum, &s.CreatedAt, &s.UpdatedAt); err != nil {
			return nil, fmt.Errorf("postgres: StatementRepo.ListByAccount: scan: %w", err)
		}
		statements = append(statements, s)
	}
	return statements, rows.Err()
}

func (r *StatementRepo) ExistsByChecksum(ctx context.Context, tenantID uuid.UUID, accountID uuid.UUID, checksum string) (bool, error) {
	tx, err := requireTx(ctx, tenantID)
	if err != nil {
		return false, err
	}

	var exists bool
	err = tx.QueryRow(ctx,
		`SELECT EXISTS (SELECT 1 FROM statements WHERE account_id = $1 AND checksum = $2)`,
		accountID, checksum,
	).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("postgres: StatementRepo.ExistsByChecksum: %w", err)
	}
	return exists, nil
}

var _ domain.StatementRepository = (*StatementRepo)(nil)
