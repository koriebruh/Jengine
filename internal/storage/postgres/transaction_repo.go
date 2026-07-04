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

// TransactionRepo implements domain.TransactionRepository.
type TransactionRepo struct{}

func NewTransactionRepo() *TransactionRepo {
	return &TransactionRepo{}
}

var transactionColumns = []string{
	"id", "tenant_id", "account_id", "statement_id", "external_ref",
	"amount", "currency", "fx_rate_to_base", "base_amount",
	"value_date", "booking_date", "description", "counterparty_ref",
	"transaction_type", "side", "source_mode", "ingestion_idempotency_key",
	"status", "raw_payload",
}

func (r *TransactionRepo) Create(ctx context.Context, tenantID uuid.UUID, t domain.Transaction) (domain.Transaction, error) {
	tx, err := requireTx(ctx, tenantID)
	if err != nil {
		return domain.Transaction{}, err
	}

	if t.ID == uuid.Nil {
		t.ID = uuid.New()
	}
	err = tx.QueryRow(ctx,
		`INSERT INTO transactions (id, tenant_id, account_id, statement_id, external_ref, amount, currency, fx_rate_to_base, base_amount, value_date, booking_date, description, counterparty_ref, transaction_type, side, source_mode, ingestion_idempotency_key, status, raw_payload)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, COALESCE($19, '{}'::jsonb))
		 RETURNING id, tenant_id, account_id, statement_id, COALESCE(external_ref, ''), amount, currency, fx_rate_to_base, base_amount, value_date, booking_date, COALESCE(description, ''), COALESCE(counterparty_ref, ''), COALESCE(transaction_type, ''), side, source_mode, ingestion_idempotency_key, status, raw_payload, created_at, updated_at`,
		t.ID, tenantID, t.AccountID, t.StatementID, t.ExternalRef, t.Amount, t.Currency, t.FXRateToBase, t.BaseAmount, t.ValueDate, t.BookingDate, t.Description, t.CounterpartyRef, t.TransactionType, t.Side, t.SourceMode, t.IngestionIdempotencyKey, t.Status, nullableJSON(t.RawPayload),
	).Scan(scanTransactionDest(&t)...)
	if err != nil {
		return domain.Transaction{}, fmt.Errorf("postgres: TransactionRepo.Create: %w", err)
	}
	return t, nil
}

func (r *TransactionRepo) GetByID(ctx context.Context, tenantID uuid.UUID, id uuid.UUID) (domain.Transaction, error) {
	tx, err := requireTx(ctx, tenantID)
	if err != nil {
		return domain.Transaction{}, err
	}

	var t domain.Transaction
	err = tx.QueryRow(ctx,
		`SELECT id, tenant_id, account_id, statement_id, COALESCE(external_ref, ''), amount, currency, fx_rate_to_base, base_amount, value_date, booking_date, COALESCE(description, ''), COALESCE(counterparty_ref, ''), COALESCE(transaction_type, ''), side, source_mode, ingestion_idempotency_key, status, raw_payload, created_at, updated_at
		 FROM transactions WHERE id = $1`,
		id,
	).Scan(scanTransactionDest(&t)...)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.Transaction{}, ErrNotFound
	}
	if err != nil {
		return domain.Transaction{}, fmt.Errorf("postgres: TransactionRepo.GetByID: %w", err)
	}
	return t, nil
}

func (r *TransactionRepo) ListUnmatched(ctx context.Context, tenantID uuid.UUID, accountID uuid.UUID, valueDateFrom, valueDateTo time.Time) ([]domain.Transaction, error) {
	tx, err := requireTx(ctx, tenantID)
	if err != nil {
		return nil, err
	}

	rows, err := tx.Query(ctx,
		`SELECT id, tenant_id, account_id, statement_id, COALESCE(external_ref, ''), amount, currency, fx_rate_to_base, base_amount, value_date, booking_date, COALESCE(description, ''), COALESCE(counterparty_ref, ''), COALESCE(transaction_type, ''), side, source_mode, ingestion_idempotency_key, status, raw_payload, created_at, updated_at
		 FROM transactions
		 WHERE account_id = $1 AND status = $2 AND value_date BETWEEN $3 AND $4
		 ORDER BY value_date`,
		accountID, domain.TransactionStatusUnmatched, valueDateFrom, valueDateTo,
	)
	if err != nil {
		return nil, fmt.Errorf("postgres: TransactionRepo.ListUnmatched: %w", err)
	}
	defer rows.Close()

	var out []domain.Transaction
	for rows.Next() {
		var t domain.Transaction
		if err := rows.Scan(scanTransactionDest(&t)...); err != nil {
			return nil, fmt.Errorf("postgres: TransactionRepo.ListUnmatched: scan: %w", err)
		}
		out = append(out, t)
	}
	return out, rows.Err()
}

func (r *TransactionRepo) UpdateStatus(ctx context.Context, tenantID uuid.UUID, id uuid.UUID, status domain.TransactionStatus) error {
	tx, err := requireTx(ctx, tenantID)
	if err != nil {
		return err
	}

	tag, err := tx.Exec(ctx,
		`UPDATE transactions SET status = $1, updated_at = now() WHERE id = $2`,
		status, id,
	)
	if err != nil {
		return fmt.Errorf("postgres: TransactionRepo.UpdateStatus: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

// BulkUpdateStatus updates every listed transaction in a single
// UPDATE ... WHERE id = ANY($1) - not per-row (plans/task/core/12
// Implementation Notes).
func (r *TransactionRepo) BulkUpdateStatus(ctx context.Context, tenantID uuid.UUID, ids []uuid.UUID, status domain.TransactionStatus) error {
	tx, err := requireTx(ctx, tenantID)
	if err != nil {
		return err
	}
	if len(ids) == 0 {
		return nil
	}

	_, err = tx.Exec(ctx,
		`UPDATE transactions SET status = $1, updated_at = now() WHERE id = ANY($2)`,
		status, ids,
	)
	if err != nil {
		return fmt.Errorf("postgres: TransactionRepo.BulkUpdateStatus: %w", err)
	}
	return nil
}

// BulkInsert uses chunked multi-row INSERT, not row-by-row, per
// plans/docs/04-matching-engine.md §5.5's explicit performance
// requirement - retrofitting this after callers depend on a row-by-row
// signature would be expensive (plans/task/core/05 Common Pitfalls).
// Not pgx.CopyFrom/COPY: Postgres rejects COPY FROM on RLS-enabled
// tables (see bulkInsertChunked's doc comment) - transactions has RLS
// enabled (plans/task/core/03), so COPY is not an option here.
func (r *TransactionRepo) BulkInsert(ctx context.Context, tenantID uuid.UUID, txs []domain.Transaction) (int, error) {
	tx, err := requireTx(ctx, tenantID)
	if err != nil {
		return 0, err
	}

	rows := make([][]any, len(txs))
	for i, t := range txs {
		id := t.ID
		if id == uuid.Nil {
			id = uuid.New()
		}
		rows[i] = []any{
			id, tenantID, t.AccountID, t.StatementID, t.ExternalRef,
			t.Amount, t.Currency, t.FXRateToBase, t.BaseAmount,
			t.ValueDate, t.BookingDate, t.Description, t.CounterpartyRef,
			t.TransactionType, t.Side, t.SourceMode, t.IngestionIdempotencyKey,
			t.Status, jsonOrEmptyObject(t.RawPayload),
		}
	}

	n, err := bulkInsertChunked(ctx, tx, "transactions", transactionColumns, rows)
	if err != nil {
		return 0, fmt.Errorf("postgres: TransactionRepo.BulkInsert: %w", err)
	}
	return n, nil
}

func (r *TransactionRepo) ExistsByIdempotencyKey(ctx context.Context, tenantID uuid.UUID, key string) (bool, error) {
	tx, err := requireTx(ctx, tenantID)
	if err != nil {
		return false, err
	}

	var exists bool
	err = tx.QueryRow(ctx,
		`SELECT EXISTS (SELECT 1 FROM transactions WHERE ingestion_idempotency_key = $1)`,
		key,
	).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("postgres: TransactionRepo.ExistsByIdempotencyKey: %w", err)
	}
	return exists, nil
}

func scanTransactionDest(t *domain.Transaction) []any {
	return []any{
		&t.ID, &t.TenantID, &t.AccountID, &t.StatementID, &t.ExternalRef,
		&t.Amount, &t.Currency, &t.FXRateToBase, &t.BaseAmount,
		&t.ValueDate, &t.BookingDate, &t.Description, &t.CounterpartyRef,
		&t.TransactionType, &t.Side, &t.SourceMode, &t.IngestionIdempotencyKey,
		&t.Status, &t.RawPayload, &t.CreatedAt, &t.UpdatedAt,
	}
}

var _ domain.TransactionRepository = (*TransactionRepo)(nil)
