package postgres

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/koriebruh/Jengine/internal/domain"
)

// TransactionRepo implements domain.TransactionRepository.
//
// RawPayloadKey encrypts/decrypts the raw_payload column at rest via
// pgcrypto's pgp_sym_encrypt/pgp_sym_decrypt (plans/task/core/23 §10.1 -
// see migrations/0013's own comment on why this column specifically).
// Resolved from RAW_PAYLOAD_ENCRYPTION_KEY the same way every other
// secret in this codebase defaults in local dev (env var - see
// connector/sftp.EnvSecretResolver's own doc comment on this being a
// stand-in for a real Vault-backed resolver); falls back to a fixed
// dev-only value so existing callers/tests that don't set the env var
// keep working, never to leaving raw_payload unencrypted.
type TransactionRepo struct {
	RawPayloadKey string
}

func NewTransactionRepo() *TransactionRepo {
	key := os.Getenv("RAW_PAYLOAD_ENCRYPTION_KEY")
	if key == "" {
		key = "dev-only-insecure-raw-payload-key"
	}
	return &TransactionRepo{RawPayloadKey: key}
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
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, pgp_sym_encrypt($19, $20))
		 RETURNING id, tenant_id, account_id, statement_id, COALESCE(external_ref, ''), amount, currency, fx_rate_to_base, base_amount, value_date, booking_date, COALESCE(description, ''), COALESCE(counterparty_ref, ''), COALESCE(transaction_type, ''), side, source_mode, ingestion_idempotency_key, status, pgp_sym_decrypt(raw_payload, $20)::text, created_at, updated_at`,
		t.ID, tenantID, t.AccountID, t.StatementID, t.ExternalRef, t.Amount, t.Currency, t.FXRateToBase, t.BaseAmount, t.ValueDate, t.BookingDate, t.Description, t.CounterpartyRef, t.TransactionType, t.Side, t.SourceMode, t.IngestionIdempotencyKey, t.Status, rawPayloadOrEmpty(t.RawPayload), r.RawPayloadKey,
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
		`SELECT id, tenant_id, account_id, statement_id, COALESCE(external_ref, ''), amount, currency, fx_rate_to_base, base_amount, value_date, booking_date, COALESCE(description, ''), COALESCE(counterparty_ref, ''), COALESCE(transaction_type, ''), side, source_mode, ingestion_idempotency_key, status, pgp_sym_decrypt(raw_payload, $2)::text, created_at, updated_at
		 FROM transactions WHERE id = $1`,
		id, r.RawPayloadKey,
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
		`SELECT id, tenant_id, account_id, statement_id, COALESCE(external_ref, ''), amount, currency, fx_rate_to_base, base_amount, value_date, booking_date, COALESCE(description, ''), COALESCE(counterparty_ref, ''), COALESCE(transaction_type, ''), side, source_mode, ingestion_idempotency_key, status, pgp_sym_decrypt(raw_payload, $5)::text, created_at, updated_at
		 FROM transactions
		 WHERE account_id = $1 AND status = $2 AND value_date BETWEEN $3 AND $4
		 ORDER BY value_date`,
		accountID, domain.TransactionStatusUnmatched, valueDateFrom, valueDateTo, r.RawPayloadKey,
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

// ListByFilter supports plans/task/core/15's ListTransactions endpoint -
// a generic filtered listing (status/date-range optional), unlike
// ListUnmatched's hardcoded status=UNMATCHED for the matching engine.
func (r *TransactionRepo) ListByFilter(ctx context.Context, tenantID uuid.UUID, accountID uuid.UUID, status domain.TransactionStatus, valueDateFrom, valueDateTo time.Time) ([]domain.Transaction, error) {
	tx, err := requireTx(ctx, tenantID)
	if err != nil {
		return nil, err
	}

	rows, err := tx.Query(ctx,
		`SELECT id, tenant_id, account_id, statement_id, COALESCE(external_ref, ''), amount, currency, fx_rate_to_base, base_amount, value_date, booking_date, COALESCE(description, ''), COALESCE(counterparty_ref, ''), COALESCE(transaction_type, ''), side, source_mode, ingestion_idempotency_key, status, pgp_sym_decrypt(raw_payload, $5)::text, created_at, updated_at
		 FROM transactions
		 WHERE account_id = $1
		   AND ($2 = '' OR status = $2)
		   AND ($3::date IS NULL OR value_date >= $3::date)
		   AND ($4::date IS NULL OR value_date <= $4::date)
		 ORDER BY value_date`,
		accountID, status, nullableTime(valueDateFrom), nullableTime(valueDateTo), r.RawPayloadKey,
	)
	if err != nil {
		return nil, fmt.Errorf("postgres: TransactionRepo.ListByFilter: %w", err)
	}
	defer rows.Close()

	var out []domain.Transaction
	for rows.Next() {
		var t domain.Transaction
		if err := rows.Scan(scanTransactionDest(&t)...); err != nil {
			return nil, fmt.Errorf("postgres: TransactionRepo.ListByFilter: scan: %w", err)
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

	// One batched pgp_sym_encrypt round trip via unnest(), not per-row -
	// bulkInsertChunked's plain $N-placeholder VALUES list has no room
	// to wrap a single column in a SQL function call without breaking
	// its generic (shared with other repos) column-list contract, and a
	// per-row encrypt call would reintroduce exactly the row-by-row cost
	// this function's own doc comment says was deliberately avoided.
	plaintexts := make([]string, len(txs))
	for i, t := range txs {
		plaintexts[i] = rawPayloadOrEmpty(t.RawPayload)
	}
	encrypted, err := r.encryptRawPayloadBatch(ctx, tx, plaintexts)
	if err != nil {
		return 0, fmt.Errorf("postgres: TransactionRepo.BulkInsert: %w", err)
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
			t.Status, encrypted[i],
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

// rawPayloadOrEmpty normalizes a possibly-empty json.RawMessage to a
// valid JSON string ("{}" if empty) before encryption - pgp_sym_encrypt
// needs a concrete text argument, not a Go nil/empty-slice ambiguity.
func rawPayloadOrEmpty(b []byte) string {
	if len(b) == 0 {
		return "{}"
	}
	return string(b)
}

// encryptRawPayloadBatch encrypts every plaintext in one round trip via
// unnest() (order-preserving for a single array, per Postgres's
// documented unnest semantics) - see BulkInsert's own doc comment on
// why this isn't a per-row pgp_sym_encrypt call.
func (r *TransactionRepo) encryptRawPayloadBatch(ctx context.Context, tx pgx.Tx, plaintexts []string) ([][]byte, error) {
	if len(plaintexts) == 0 {
		return nil, nil
	}
	rows, err := tx.Query(ctx, `SELECT pgp_sym_encrypt(p, $2) FROM unnest($1::text[]) AS p`, plaintexts, r.RawPayloadKey)
	if err != nil {
		return nil, fmt.Errorf("encrypt raw_payload batch: %w", err)
	}
	defer rows.Close()

	out := make([][]byte, 0, len(plaintexts))
	for rows.Next() {
		var ciphertext []byte
		if err := rows.Scan(&ciphertext); err != nil {
			return nil, fmt.Errorf("encrypt raw_payload batch: scan: %w", err)
		}
		out = append(out, ciphertext)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("encrypt raw_payload batch: %w", err)
	}
	if len(out) != len(plaintexts) {
		return nil, fmt.Errorf("encrypt raw_payload batch: expected %d ciphertexts, got %d", len(plaintexts), len(out))
	}
	return out, nil
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
