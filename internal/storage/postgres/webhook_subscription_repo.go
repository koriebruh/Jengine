package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/koriebruh/Jengine/internal/domain"
)

// WebhookSubscriptionRepo implements domain.WebhookSubscriptionRepository
// (plans/task/core/21).
type WebhookSubscriptionRepo struct{}

func NewWebhookSubscriptionRepo() *WebhookSubscriptionRepo { return &WebhookSubscriptionRepo{} }

func (r *WebhookSubscriptionRepo) Create(ctx context.Context, tenantID uuid.UUID, s domain.WebhookSubscription) (domain.WebhookSubscription, error) {
	tx, err := requireTx(ctx, tenantID)
	if err != nil {
		return domain.WebhookSubscription{}, err
	}
	if s.Status == "" {
		s.Status = domain.WebhookSubscriptionStatusActive
	}
	err = tx.QueryRow(ctx,
		`INSERT INTO webhook_subscriptions (tenant_id, url, secret_ref, event_types, filter_expr, status)
		 VALUES ($1, $2, $3, $4, $5, $6)
		 RETURNING id, created_at, updated_at`,
		tenantID, s.URL, s.SecretRef, s.EventTypes, s.FilterExpr, s.Status,
	).Scan(&s.ID, &s.CreatedAt, &s.UpdatedAt)
	if err != nil {
		return domain.WebhookSubscription{}, fmt.Errorf("postgres: WebhookSubscriptionRepo.Create: %w", err)
	}
	s.TenantID = tenantID
	return s, nil
}

func (r *WebhookSubscriptionRepo) GetByID(ctx context.Context, tenantID uuid.UUID, id uuid.UUID) (domain.WebhookSubscription, error) {
	tx, err := requireTx(ctx, tenantID)
	if err != nil {
		return domain.WebhookSubscription{}, err
	}
	var s domain.WebhookSubscription
	err = tx.QueryRow(ctx,
		`SELECT id, tenant_id, url, secret_ref, event_types, filter_expr, status, created_at, updated_at
		 FROM webhook_subscriptions WHERE id = $1`, id,
	).Scan(&s.ID, &s.TenantID, &s.URL, &s.SecretRef, &s.EventTypes, &s.FilterExpr, &s.Status, &s.CreatedAt, &s.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.WebhookSubscription{}, ErrNotFound
	}
	if err != nil {
		return domain.WebhookSubscription{}, fmt.Errorf("postgres: WebhookSubscriptionRepo.GetByID: %w", err)
	}
	return s, nil
}

func (r *WebhookSubscriptionRepo) ListByTenant(ctx context.Context, tenantID uuid.UUID) ([]domain.WebhookSubscription, error) {
	tx, err := requireTx(ctx, tenantID)
	if err != nil {
		return nil, err
	}
	rows, err := tx.Query(ctx,
		`SELECT id, tenant_id, url, secret_ref, event_types, filter_expr, status, created_at, updated_at
		 FROM webhook_subscriptions ORDER BY created_at`,
	)
	if err != nil {
		return nil, fmt.Errorf("postgres: WebhookSubscriptionRepo.ListByTenant: %w", err)
	}
	defer rows.Close()

	var subs []domain.WebhookSubscription
	for rows.Next() {
		var s domain.WebhookSubscription
		if err := rows.Scan(&s.ID, &s.TenantID, &s.URL, &s.SecretRef, &s.EventTypes, &s.FilterExpr, &s.Status, &s.CreatedAt, &s.UpdatedAt); err != nil {
			return nil, fmt.Errorf("postgres: WebhookSubscriptionRepo.ListByTenant: scan: %w", err)
		}
		subs = append(subs, s)
	}
	return subs, rows.Err()
}

func (r *WebhookSubscriptionRepo) ListActiveByEventType(ctx context.Context, tenantID uuid.UUID, eventType string) ([]domain.WebhookSubscription, error) {
	tx, err := requireTx(ctx, tenantID)
	if err != nil {
		return nil, err
	}
	rows, err := tx.Query(ctx,
		`SELECT id, tenant_id, url, secret_ref, event_types, filter_expr, status, created_at, updated_at
		 FROM webhook_subscriptions WHERE status = 'ACTIVE' AND $1 = ANY(event_types)`,
		eventType,
	)
	if err != nil {
		return nil, fmt.Errorf("postgres: WebhookSubscriptionRepo.ListActiveByEventType: %w", err)
	}
	defer rows.Close()

	var subs []domain.WebhookSubscription
	for rows.Next() {
		var s domain.WebhookSubscription
		if err := rows.Scan(&s.ID, &s.TenantID, &s.URL, &s.SecretRef, &s.EventTypes, &s.FilterExpr, &s.Status, &s.CreatedAt, &s.UpdatedAt); err != nil {
			return nil, fmt.Errorf("postgres: WebhookSubscriptionRepo.ListActiveByEventType: scan: %w", err)
		}
		subs = append(subs, s)
	}
	return subs, rows.Err()
}

func (r *WebhookSubscriptionRepo) UpdateStatus(ctx context.Context, tenantID uuid.UUID, id uuid.UUID, status domain.WebhookSubscriptionStatus) error {
	tx, err := requireTx(ctx, tenantID)
	if err != nil {
		return err
	}
	tag, err := tx.Exec(ctx,
		`UPDATE webhook_subscriptions SET status = $1, updated_at = now() WHERE id = $2`,
		status, id,
	)
	if err != nil {
		return fmt.Errorf("postgres: WebhookSubscriptionRepo.UpdateStatus: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

var _ domain.WebhookSubscriptionRepository = (*WebhookSubscriptionRepo)(nil)
