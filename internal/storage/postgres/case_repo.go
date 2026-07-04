package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/koriebruh/Jengine/internal/domain"
)

// CaseRepo implements domain.CaseRepository - pure CRUD/read-write over
// Case, CaseComment, and CaseAuditEvent. Lifecycle/state-machine
// transition logic is plans/task/core/13 (see Non-Goals).
type CaseRepo struct{}

func NewCaseRepo() *CaseRepo {
	return &CaseRepo{}
}

func (r *CaseRepo) Create(ctx context.Context, tenantID uuid.UUID, c domain.Case) (domain.Case, error) {
	tx, err := requireTx(ctx, tenantID)
	if err != nil {
		return domain.Case{}, err
	}

	if c.ID == uuid.Nil {
		c.ID = uuid.New()
	}
	if c.RelatedTransactionIDs == nil {
		c.RelatedTransactionIDs = []uuid.UUID{}
	}
	err = tx.QueryRow(ctx,
		`INSERT INTO cases (id, tenant_id, account_id, related_transaction_ids, break_type, root_cause_category, status, assigned_to, priority, sla_due_at, amount_at_risk, currency, temporal_workflow_id)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
		 RETURNING id, tenant_id, account_id, related_transaction_ids, break_type, root_cause_category, status, assigned_to, priority, sla_due_at, opened_at, resolved_at, amount_at_risk, currency, temporal_workflow_id, created_at, updated_at`,
		c.ID, tenantID, c.AccountID, c.RelatedTransactionIDs, c.BreakType, c.RootCauseCategory, c.Status, c.AssignedTo, c.Priority, c.SLADueAt, c.AmountAtRisk, c.Currency, c.TemporalWorkflowID,
	).Scan(scanCaseDest(&c)...)
	if err != nil {
		return domain.Case{}, fmt.Errorf("postgres: CaseRepo.Create: %w", err)
	}
	return c, nil
}

func (r *CaseRepo) GetByID(ctx context.Context, tenantID uuid.UUID, id uuid.UUID) (domain.Case, error) {
	tx, err := requireTx(ctx, tenantID)
	if err != nil {
		return domain.Case{}, err
	}

	var c domain.Case
	err = tx.QueryRow(ctx,
		`SELECT id, tenant_id, account_id, related_transaction_ids, break_type, root_cause_category, status, assigned_to, priority, sla_due_at, opened_at, resolved_at, amount_at_risk, currency, temporal_workflow_id, created_at, updated_at
		 FROM cases WHERE id = $1`,
		id,
	).Scan(scanCaseDest(&c)...)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.Case{}, ErrNotFound
	}
	if err != nil {
		return domain.Case{}, fmt.Errorf("postgres: CaseRepo.GetByID: %w", err)
	}
	return c, nil
}

func (r *CaseRepo) ListByStatus(ctx context.Context, tenantID uuid.UUID, status domain.CaseStatus) ([]domain.Case, error) {
	tx, err := requireTx(ctx, tenantID)
	if err != nil {
		return nil, err
	}

	rows, err := tx.Query(ctx,
		`SELECT id, tenant_id, account_id, related_transaction_ids, break_type, root_cause_category, status, assigned_to, priority, sla_due_at, opened_at, resolved_at, amount_at_risk, currency, temporal_workflow_id, created_at, updated_at
		 FROM cases WHERE status = $1 ORDER BY opened_at`,
		status,
	)
	if err != nil {
		return nil, fmt.Errorf("postgres: CaseRepo.ListByStatus: %w", err)
	}
	defer rows.Close()

	var cases []domain.Case
	for rows.Next() {
		var c domain.Case
		if err := rows.Scan(scanCaseDest(&c)...); err != nil {
			return nil, fmt.Errorf("postgres: CaseRepo.ListByStatus: scan: %w", err)
		}
		cases = append(cases, c)
	}
	return cases, rows.Err()
}

func (r *CaseRepo) UpdateStatus(ctx context.Context, tenantID uuid.UUID, id uuid.UUID, status domain.CaseStatus) error {
	tx, err := requireTx(ctx, tenantID)
	if err != nil {
		return err
	}

	tag, err := tx.Exec(ctx,
		`UPDATE cases SET status = $1, updated_at = now(),
		   resolved_at = CASE WHEN $1 IN ('RESOLVED', 'WRITTEN_OFF') THEN now() ELSE resolved_at END
		 WHERE id = $2`,
		status, id,
	)
	if err != nil {
		return fmt.Errorf("postgres: CaseRepo.UpdateStatus: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

func (r *CaseRepo) UpdateRootCause(ctx context.Context, tenantID uuid.UUID, id uuid.UUID, category string) error {
	tx, err := requireTx(ctx, tenantID)
	if err != nil {
		return err
	}

	tag, err := tx.Exec(ctx,
		`UPDATE cases SET root_cause_category = $1, updated_at = now() WHERE id = $2`,
		category, id,
	)
	if err != nil {
		return fmt.Errorf("postgres: CaseRepo.UpdateRootCause: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

func (r *CaseRepo) UpdateAssignee(ctx context.Context, tenantID uuid.UUID, id uuid.UUID, assignee string) error {
	tx, err := requireTx(ctx, tenantID)
	if err != nil {
		return err
	}

	tag, err := tx.Exec(ctx,
		`UPDATE cases SET assigned_to = $1, updated_at = now() WHERE id = $2`,
		assignee, id,
	)
	if err != nil {
		return fmt.Errorf("postgres: CaseRepo.UpdateAssignee: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

func (r *CaseRepo) AddComment(ctx context.Context, tenantID uuid.UUID, c domain.CaseComment) (domain.CaseComment, error) {
	tx, err := requireTx(ctx, tenantID)
	if err != nil {
		return domain.CaseComment{}, err
	}

	if c.ID == uuid.Nil {
		c.ID = uuid.New()
	}
	if c.EventType == "" {
		c.EventType = "comment"
	}
	err = tx.QueryRow(ctx,
		`INSERT INTO case_comments (id, tenant_id, case_id, actor, event_type, payload)
		 VALUES ($1, $2, $3, $4, $5, COALESCE($6, '{}'::jsonb))
		 RETURNING id, tenant_id, case_id, actor, event_type, payload, created_at`,
		c.ID, tenantID, c.CaseID, c.Actor, c.EventType, nullableJSON(c.Payload),
	).Scan(&c.ID, &c.TenantID, &c.CaseID, &c.Actor, &c.EventType, &c.Payload, &c.CreatedAt)
	if err != nil {
		return domain.CaseComment{}, fmt.Errorf("postgres: CaseRepo.AddComment: %w", err)
	}
	return c, nil
}

func (r *CaseRepo) ListComments(ctx context.Context, tenantID uuid.UUID, caseID uuid.UUID) ([]domain.CaseComment, error) {
	tx, err := requireTx(ctx, tenantID)
	if err != nil {
		return nil, err
	}

	rows, err := tx.Query(ctx,
		`SELECT id, tenant_id, case_id, actor, event_type, payload, created_at FROM case_comments WHERE case_id = $1 ORDER BY created_at`,
		caseID,
	)
	if err != nil {
		return nil, fmt.Errorf("postgres: CaseRepo.ListComments: %w", err)
	}
	defer rows.Close()

	var comments []domain.CaseComment
	for rows.Next() {
		var c domain.CaseComment
		if err := rows.Scan(&c.ID, &c.TenantID, &c.CaseID, &c.Actor, &c.EventType, &c.Payload, &c.CreatedAt); err != nil {
			return nil, fmt.Errorf("postgres: CaseRepo.ListComments: scan: %w", err)
		}
		comments = append(comments, c)
	}
	return comments, rows.Err()
}

func (r *CaseRepo) AddAuditEvent(ctx context.Context, tenantID uuid.UUID, e domain.CaseAuditEvent) (domain.CaseAuditEvent, error) {
	tx, err := requireTx(ctx, tenantID)
	if err != nil {
		return domain.CaseAuditEvent{}, err
	}

	if e.ID == uuid.Nil {
		e.ID = uuid.New()
	}
	err = tx.QueryRow(ctx,
		`INSERT INTO case_audit_events (id, tenant_id, case_id, actor, event_type, payload)
		 VALUES ($1, $2, $3, $4, $5, COALESCE($6, '{}'::jsonb))
		 RETURNING id, tenant_id, case_id, actor, event_type, payload, created_at`,
		e.ID, tenantID, e.CaseID, e.Actor, e.EventType, nullableJSON(e.Payload),
	).Scan(&e.ID, &e.TenantID, &e.CaseID, &e.Actor, &e.EventType, &e.Payload, &e.CreatedAt)
	if err != nil {
		return domain.CaseAuditEvent{}, fmt.Errorf("postgres: CaseRepo.AddAuditEvent: %w", err)
	}
	return e, nil
}

func (r *CaseRepo) ListAuditEvents(ctx context.Context, tenantID uuid.UUID, caseID uuid.UUID) ([]domain.CaseAuditEvent, error) {
	tx, err := requireTx(ctx, tenantID)
	if err != nil {
		return nil, err
	}

	rows, err := tx.Query(ctx,
		`SELECT id, tenant_id, case_id, actor, event_type, payload, created_at FROM case_audit_events WHERE case_id = $1 ORDER BY created_at`,
		caseID,
	)
	if err != nil {
		return nil, fmt.Errorf("postgres: CaseRepo.ListAuditEvents: %w", err)
	}
	defer rows.Close()

	var events []domain.CaseAuditEvent
	for rows.Next() {
		var e domain.CaseAuditEvent
		if err := rows.Scan(&e.ID, &e.TenantID, &e.CaseID, &e.Actor, &e.EventType, &e.Payload, &e.CreatedAt); err != nil {
			return nil, fmt.Errorf("postgres: CaseRepo.ListAuditEvents: scan: %w", err)
		}
		events = append(events, e)
	}
	return events, rows.Err()
}

func scanCaseDest(c *domain.Case) []any {
	return []any{
		&c.ID, &c.TenantID, &c.AccountID, &c.RelatedTransactionIDs, &c.BreakType,
		&c.RootCauseCategory, &c.Status, &c.AssignedTo, &c.Priority, &c.SLADueAt,
		&c.OpenedAt, &c.ResolvedAt, &c.AmountAtRisk, &c.Currency, &c.TemporalWorkflowID,
		&c.CreatedAt, &c.UpdatedAt,
	}
}

var _ domain.CaseRepository = (*CaseRepo)(nil)
