package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/koriebruh/Jengine/internal/domain"
)

// MatchResultRepo implements domain.MatchResultRepository. Result and
// lines are always written/read together (one result, many lines) - see
// plans/task/core/05 Common Pitfalls; never collapsed into one
// denormalized access pattern.
type MatchResultRepo struct{}

func NewMatchResultRepo() *MatchResultRepo {
	return &MatchResultRepo{}
}

func (r *MatchResultRepo) Create(ctx context.Context, tenantID uuid.UUID, result domain.MatchResult, lines []domain.MatchResultLine) (domain.MatchResult, error) {
	tx, err := requireTx(ctx, tenantID)
	if err != nil {
		return domain.MatchResult{}, err
	}

	if result.ID == uuid.Nil {
		result.ID = uuid.New()
	}
	err = tx.QueryRow(ctx,
		`INSERT INTO match_results (id, tenant_id, rule_id, match_type, confidence_score, status, matched_at, matched_by, amount_variance, date_variance)
		 VALUES ($1, $2, $3, $4, $5, $6, COALESCE($7, now()), $8, $9, $10)
		 RETURNING id, tenant_id, rule_id, match_type, confidence_score, status, matched_at, matched_by, amount_variance, date_variance, created_at`,
		result.ID, tenantID, result.RuleID, result.MatchType, result.ConfidenceScore, result.Status, nullableTime(result.MatchedAt), result.MatchedBy, result.AmountVariance, result.DateVariance,
	).Scan(&result.ID, &result.TenantID, &result.RuleID, &result.MatchType, &result.ConfidenceScore, &result.Status, &result.MatchedAt, &result.MatchedBy, &result.AmountVariance, &result.DateVariance, &result.CreatedAt)
	if err != nil {
		return domain.MatchResult{}, fmt.Errorf("postgres: MatchResultRepo.Create: insert result: %w", err)
	}

	rows := make([][]any, len(lines))
	for i, line := range lines {
		rows[i] = []any{result.ID, line.TransactionID, tenantID, line.Side, line.AllocatedAmount}
	}
	if len(rows) > 0 {
		// Not pgx.CopyFrom/COPY - match_result_lines has RLS enabled and
		// Postgres rejects COPY FROM on RLS-enabled tables, see
		// bulkInsertChunked's doc comment.
		if _, err := bulkInsertChunked(ctx, tx, "match_result_lines",
			[]string{"match_result_id", "transaction_id", "tenant_id", "side", "allocated_amount"},
			rows,
		); err != nil {
			return domain.MatchResult{}, fmt.Errorf("postgres: MatchResultRepo.Create: insert lines: %w", err)
		}
	}

	return result, nil
}

func (r *MatchResultRepo) GetByID(ctx context.Context, tenantID uuid.UUID, id uuid.UUID) (domain.MatchResult, []domain.MatchResultLine, error) {
	tx, err := requireTx(ctx, tenantID)
	if err != nil {
		return domain.MatchResult{}, nil, err
	}

	var result domain.MatchResult
	err = tx.QueryRow(ctx,
		`SELECT id, tenant_id, rule_id, match_type, confidence_score, status, matched_at, matched_by, amount_variance, date_variance, created_at
		 FROM match_results WHERE id = $1`,
		id,
	).Scan(&result.ID, &result.TenantID, &result.RuleID, &result.MatchType, &result.ConfidenceScore, &result.Status, &result.MatchedAt, &result.MatchedBy, &result.AmountVariance, &result.DateVariance, &result.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.MatchResult{}, nil, ErrNotFound
	}
	if err != nil {
		return domain.MatchResult{}, nil, fmt.Errorf("postgres: MatchResultRepo.GetByID: %w", err)
	}

	rows, err := tx.Query(ctx,
		`SELECT match_result_id, transaction_id, tenant_id, side, allocated_amount FROM match_result_lines WHERE match_result_id = $1`,
		id,
	)
	if err != nil {
		return domain.MatchResult{}, nil, fmt.Errorf("postgres: MatchResultRepo.GetByID: lines: %w", err)
	}
	defer rows.Close()

	var lines []domain.MatchResultLine
	for rows.Next() {
		var l domain.MatchResultLine
		if err := rows.Scan(&l.MatchResultID, &l.TransactionID, &l.TenantID, &l.Side, &l.AllocatedAmount); err != nil {
			return domain.MatchResult{}, nil, fmt.Errorf("postgres: MatchResultRepo.GetByID: scan line: %w", err)
		}
		lines = append(lines, l)
	}
	return result, lines, rows.Err()
}

func (r *MatchResultRepo) ListByStatus(ctx context.Context, tenantID uuid.UUID, status domain.MatchResultStatus) ([]domain.MatchResult, error) {
	tx, err := requireTx(ctx, tenantID)
	if err != nil {
		return nil, err
	}

	rows, err := tx.Query(ctx,
		`SELECT id, tenant_id, rule_id, match_type, confidence_score, status, matched_at, matched_by, amount_variance, date_variance, created_at
		 FROM match_results WHERE status = $1 ORDER BY matched_at`,
		status,
	)
	if err != nil {
		return nil, fmt.Errorf("postgres: MatchResultRepo.ListByStatus: %w", err)
	}
	defer rows.Close()

	var results []domain.MatchResult
	for rows.Next() {
		var result domain.MatchResult
		if err := rows.Scan(&result.ID, &result.TenantID, &result.RuleID, &result.MatchType, &result.ConfidenceScore, &result.Status, &result.MatchedAt, &result.MatchedBy, &result.AmountVariance, &result.DateVariance, &result.CreatedAt); err != nil {
			return nil, fmt.Errorf("postgres: MatchResultRepo.ListByStatus: scan: %w", err)
		}
		results = append(results, result)
	}
	return results, rows.Err()
}

func (r *MatchResultRepo) UpdateStatus(ctx context.Context, tenantID uuid.UUID, id uuid.UUID, status domain.MatchResultStatus, matchedBy *string) error {
	tx, err := requireTx(ctx, tenantID)
	if err != nil {
		return err
	}

	tag, err := tx.Exec(ctx,
		`UPDATE match_results SET status = $1, matched_by = COALESCE($2, matched_by) WHERE id = $3`,
		status, matchedBy, id,
	)
	if err != nil {
		return fmt.Errorf("postgres: MatchResultRepo.UpdateStatus: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

var _ domain.MatchResultRepository = (*MatchResultRepo)(nil)
