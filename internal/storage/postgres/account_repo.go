package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/koriebruh/Jengine/internal/domain"
)

// ErrNotFound is returned by repository GetByID methods when no matching
// row exists (or RLS hides it, which looks identical from the caller's
// side - that's the point).
var ErrNotFound = errors.New("postgres: not found")

// AccountRepo implements domain.AccountRepository.
type AccountRepo struct{}

func NewAccountRepo() *AccountRepo {
	return &AccountRepo{}
}

func (r *AccountRepo) Create(ctx context.Context, tenantID uuid.UUID, a domain.Account) (domain.Account, error) {
	tx, err := requireTx(ctx, tenantID)
	if err != nil {
		return domain.Account{}, err
	}

	if a.ID == uuid.Nil {
		a.ID = uuid.New()
	}
	err = tx.QueryRow(ctx,
		`INSERT INTO accounts (id, tenant_id, external_account_ref, account_type, currency, name, metadata)
		 VALUES ($1, $2, $3, $4, $5, $6, COALESCE($7, '{}'::jsonb))
		 RETURNING id, tenant_id, external_account_ref, account_type, currency, name, metadata, created_at, updated_at`,
		a.ID, tenantID, a.ExternalAccountRef, a.AccountType, a.Currency, a.Name, nullableJSON(a.Metadata),
	).Scan(&a.ID, &a.TenantID, &a.ExternalAccountRef, &a.AccountType, &a.Currency, &a.Name, &a.Metadata, &a.CreatedAt, &a.UpdatedAt)
	if err != nil {
		return domain.Account{}, fmt.Errorf("postgres: AccountRepo.Create: %w", err)
	}
	return a, nil
}

func (r *AccountRepo) GetByID(ctx context.Context, tenantID uuid.UUID, id uuid.UUID) (domain.Account, error) {
	tx, err := requireTx(ctx, tenantID)
	if err != nil {
		return domain.Account{}, err
	}

	var a domain.Account
	err = tx.QueryRow(ctx,
		`SELECT id, tenant_id, external_account_ref, account_type, currency, name, metadata, created_at, updated_at
		 FROM accounts WHERE id = $1`,
		id,
	).Scan(&a.ID, &a.TenantID, &a.ExternalAccountRef, &a.AccountType, &a.Currency, &a.Name, &a.Metadata, &a.CreatedAt, &a.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.Account{}, ErrNotFound
	}
	if err != nil {
		return domain.Account{}, fmt.Errorf("postgres: AccountRepo.GetByID: %w", err)
	}
	return a, nil
}

func (r *AccountRepo) ListByTenant(ctx context.Context, tenantID uuid.UUID) ([]domain.Account, error) {
	tx, err := requireTx(ctx, tenantID)
	if err != nil {
		return nil, err
	}

	rows, err := tx.Query(ctx,
		`SELECT id, tenant_id, external_account_ref, account_type, currency, name, metadata, created_at, updated_at
		 FROM accounts ORDER BY created_at`,
	)
	if err != nil {
		return nil, fmt.Errorf("postgres: AccountRepo.ListByTenant: %w", err)
	}
	defer rows.Close()

	var accounts []domain.Account
	for rows.Next() {
		var a domain.Account
		if err := rows.Scan(&a.ID, &a.TenantID, &a.ExternalAccountRef, &a.AccountType, &a.Currency, &a.Name, &a.Metadata, &a.CreatedAt, &a.UpdatedAt); err != nil {
			return nil, fmt.Errorf("postgres: AccountRepo.ListByTenant: scan: %w", err)
		}
		accounts = append(accounts, a)
	}
	return accounts, rows.Err()
}

// nullableJSON returns nil if b is empty, so the SQL's COALESCE(...,
// '{}'::jsonb) kicks in instead of pgx trying to send a zero-length,
// invalid JSON value.
func nullableJSON(b []byte) any {
	if len(b) == 0 {
		return nil
	}
	return b
}

var _ domain.AccountRepository = (*AccountRepo)(nil)
