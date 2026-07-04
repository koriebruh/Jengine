package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/koriebruh/Jengine/internal/domain"
)

// MatchRuleRepo implements domain.MatchRuleRepository. Only persists/reads
// RuleSpec as opaque jsonb - interpreting it is plans/task/core/11's job
// (see Non-Goals).
type MatchRuleRepo struct{}

func NewMatchRuleRepo() *MatchRuleRepo {
	return &MatchRuleRepo{}
}

func (r *MatchRuleRepo) Create(ctx context.Context, tenantID uuid.UUID, rule domain.MatchRule) (domain.MatchRule, error) {
	tx, err := requireTx(ctx, tenantID)
	if err != nil {
		return domain.MatchRule{}, err
	}

	if rule.ID == uuid.Nil {
		rule.ID = uuid.New()
	}
	err = tx.QueryRow(ctx,
		`INSERT INTO match_rules (id, tenant_id, name, version, status, rule_spec, match_type, source_account_id, target_account_id, priority, auto_match_threshold, created_by, approved_by, effective_from)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
		 RETURNING id, tenant_id, name, version, status, rule_spec, match_type, source_account_id, target_account_id, priority, auto_match_threshold, created_by, approved_by, effective_from, created_at, updated_at`,
		rule.ID, tenantID, rule.Name, rule.Version, rule.Status, nullableJSON(rule.RuleSpec), rule.MatchType, rule.SourceAccountID, rule.TargetAccountID, rule.Priority, rule.AutoMatchThreshold, rule.CreatedBy, rule.ApprovedBy, rule.EffectiveFrom,
	).Scan(&rule.ID, &rule.TenantID, &rule.Name, &rule.Version, &rule.Status, &rule.RuleSpec, &rule.MatchType, &rule.SourceAccountID, &rule.TargetAccountID, &rule.Priority, &rule.AutoMatchThreshold, &rule.CreatedBy, &rule.ApprovedBy, &rule.EffectiveFrom, &rule.CreatedAt, &rule.UpdatedAt)
	if err != nil {
		return domain.MatchRule{}, fmt.Errorf("postgres: MatchRuleRepo.Create: %w", err)
	}
	return rule, nil
}

func (r *MatchRuleRepo) GetByID(ctx context.Context, tenantID uuid.UUID, id uuid.UUID) (domain.MatchRule, error) {
	tx, err := requireTx(ctx, tenantID)
	if err != nil {
		return domain.MatchRule{}, err
	}

	var rule domain.MatchRule
	err = tx.QueryRow(ctx,
		`SELECT id, tenant_id, name, version, status, rule_spec, match_type, source_account_id, target_account_id, priority, auto_match_threshold, created_by, approved_by, effective_from, created_at, updated_at
		 FROM match_rules WHERE id = $1`,
		id,
	).Scan(&rule.ID, &rule.TenantID, &rule.Name, &rule.Version, &rule.Status, &rule.RuleSpec, &rule.MatchType, &rule.SourceAccountID, &rule.TargetAccountID, &rule.Priority, &rule.AutoMatchThreshold, &rule.CreatedBy, &rule.ApprovedBy, &rule.EffectiveFrom, &rule.CreatedAt, &rule.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.MatchRule{}, ErrNotFound
	}
	if err != nil {
		return domain.MatchRule{}, fmt.Errorf("postgres: MatchRuleRepo.GetByID: %w", err)
	}
	return rule, nil
}

// ListActive returns ACTIVE rules for an account pair, ordered by
// priority ascending - plans/docs/04-matching-engine.md §5.1's rule
// chaining runs lower-priority rules first.
func (r *MatchRuleRepo) ListActive(ctx context.Context, tenantID uuid.UUID, sourceAccountID, targetAccountID uuid.UUID) ([]domain.MatchRule, error) {
	tx, err := requireTx(ctx, tenantID)
	if err != nil {
		return nil, err
	}

	rows, err := tx.Query(ctx,
		`SELECT id, tenant_id, name, version, status, rule_spec, match_type, source_account_id, target_account_id, priority, auto_match_threshold, created_by, approved_by, effective_from, created_at, updated_at
		 FROM match_rules
		 WHERE status = $1 AND source_account_id = $2 AND target_account_id = $3
		 ORDER BY priority ASC`,
		domain.MatchRuleStatusActive, sourceAccountID, targetAccountID,
	)
	if err != nil {
		return nil, fmt.Errorf("postgres: MatchRuleRepo.ListActive: %w", err)
	}
	defer rows.Close()

	var rules []domain.MatchRule
	for rows.Next() {
		var rule domain.MatchRule
		if err := rows.Scan(&rule.ID, &rule.TenantID, &rule.Name, &rule.Version, &rule.Status, &rule.RuleSpec, &rule.MatchType, &rule.SourceAccountID, &rule.TargetAccountID, &rule.Priority, &rule.AutoMatchThreshold, &rule.CreatedBy, &rule.ApprovedBy, &rule.EffectiveFrom, &rule.CreatedAt, &rule.UpdatedAt); err != nil {
			return nil, fmt.Errorf("postgres: MatchRuleRepo.ListActive: scan: %w", err)
		}
		rules = append(rules, rule)
	}
	return rules, rows.Err()
}

func (r *MatchRuleRepo) UpdateStatus(ctx context.Context, tenantID uuid.UUID, id uuid.UUID, status domain.MatchRuleStatus, approvedBy *string) error {
	tx, err := requireTx(ctx, tenantID)
	if err != nil {
		return err
	}

	tag, err := tx.Exec(ctx,
		`UPDATE match_rules SET status = $1, approved_by = COALESCE($2, approved_by), updated_at = now() WHERE id = $3`,
		status, approvedBy, id,
	)
	if err != nil {
		return fmt.Errorf("postgres: MatchRuleRepo.UpdateStatus: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

var _ domain.MatchRuleRepository = (*MatchRuleRepo)(nil)
