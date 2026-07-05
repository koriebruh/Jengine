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

// WebhookDeliveryRepo implements domain.WebhookDeliveryRepository
// (plans/task/core/21).
type WebhookDeliveryRepo struct{}

func NewWebhookDeliveryRepo() *WebhookDeliveryRepo { return &WebhookDeliveryRepo{} }

func (r *WebhookDeliveryRepo) Create(ctx context.Context, tenantID uuid.UUID, d domain.WebhookDelivery) (domain.WebhookDelivery, error) {
	tx, err := requireTx(ctx, tenantID)
	if err != nil {
		return domain.WebhookDelivery{}, err
	}
	if d.Status == "" {
		d.Status = domain.WebhookDeliveryStatusPending
	}
	err = tx.QueryRow(ctx,
		`INSERT INTO webhook_deliveries (tenant_id, subscription_id, event_id, event_type, payload, attempt_count, status, next_attempt_at)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		 RETURNING id, created_at`,
		tenantID, d.SubscriptionID, d.EventID, d.EventType, d.Payload, d.AttemptCount, d.Status, d.NextAttemptAt,
	).Scan(&d.ID, &d.CreatedAt)
	if err != nil {
		return domain.WebhookDelivery{}, fmt.Errorf("postgres: WebhookDeliveryRepo.Create: %w", err)
	}
	d.TenantID = tenantID
	return d, nil
}

func (r *WebhookDeliveryRepo) GetByID(ctx context.Context, tenantID uuid.UUID, id uuid.UUID) (domain.WebhookDelivery, error) {
	tx, err := requireTx(ctx, tenantID)
	if err != nil {
		return domain.WebhookDelivery{}, err
	}
	var d domain.WebhookDelivery
	err = tx.QueryRow(ctx,
		`SELECT id, tenant_id, subscription_id, event_id, event_type, payload, attempt_count, status,
		        last_attempt_at, next_attempt_at, response_status, response_body_snippet, created_at
		 FROM webhook_deliveries WHERE id = $1`, id,
	).Scan(&d.ID, &d.TenantID, &d.SubscriptionID, &d.EventID, &d.EventType, &d.Payload, &d.AttemptCount, &d.Status,
		&d.LastAttemptAt, &d.NextAttemptAt, &d.ResponseStatus, &d.ResponseBodySnippet, &d.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.WebhookDelivery{}, ErrNotFound
	}
	if err != nil {
		return domain.WebhookDelivery{}, fmt.Errorf("postgres: WebhookDeliveryRepo.GetByID: %w", err)
	}
	return d, nil
}

func (r *WebhookDeliveryRepo) ListByStatus(ctx context.Context, tenantID uuid.UUID, status domain.WebhookDeliveryStatus) ([]domain.WebhookDelivery, error) {
	tx, err := requireTx(ctx, tenantID)
	if err != nil {
		return nil, err
	}
	rows, err := tx.Query(ctx,
		`SELECT id, tenant_id, subscription_id, event_id, event_type, payload, attempt_count, status,
		        last_attempt_at, next_attempt_at, response_status, response_body_snippet, created_at
		 FROM webhook_deliveries WHERE status = $1 ORDER BY created_at DESC`, status,
	)
	if err != nil {
		return nil, fmt.Errorf("postgres: WebhookDeliveryRepo.ListByStatus: %w", err)
	}
	defer rows.Close()

	var deliveries []domain.WebhookDelivery
	for rows.Next() {
		var d domain.WebhookDelivery
		if err := rows.Scan(&d.ID, &d.TenantID, &d.SubscriptionID, &d.EventID, &d.EventType, &d.Payload, &d.AttemptCount, &d.Status,
			&d.LastAttemptAt, &d.NextAttemptAt, &d.ResponseStatus, &d.ResponseBodySnippet, &d.CreatedAt); err != nil {
			return nil, fmt.Errorf("postgres: WebhookDeliveryRepo.ListByStatus: scan: %w", err)
		}
		deliveries = append(deliveries, d)
	}
	return deliveries, rows.Err()
}

func (r *WebhookDeliveryRepo) UpdateAfterAttempt(ctx context.Context, tenantID uuid.UUID, id uuid.UUID, attemptCount int, status domain.WebhookDeliveryStatus, lastAttemptAt time.Time, nextAttemptAt *time.Time, responseStatus *int, responseBodySnippet *string) error {
	tx, err := requireTx(ctx, tenantID)
	if err != nil {
		return err
	}
	tag, err := tx.Exec(ctx,
		`UPDATE webhook_deliveries
		 SET attempt_count = $1, status = $2, last_attempt_at = $3, next_attempt_at = $4,
		     response_status = $5, response_body_snippet = $6
		 WHERE id = $7`,
		attemptCount, status, lastAttemptAt, nextAttemptAt, responseStatus, responseBodySnippet, id,
	)
	if err != nil {
		return fmt.Errorf("postgres: WebhookDeliveryRepo.UpdateAfterAttempt: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

func (r *WebhookDeliveryRepo) Redrive(ctx context.Context, tenantID uuid.UUID, id uuid.UUID) error {
	tx, err := requireTx(ctx, tenantID)
	if err != nil {
		return err
	}
	tag, err := tx.Exec(ctx,
		`UPDATE webhook_deliveries SET attempt_count = 0, status = 'PENDING', next_attempt_at = now() WHERE id = $1`,
		id,
	)
	if err != nil {
		return fmt.Errorf("postgres: WebhookDeliveryRepo.Redrive: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

var _ domain.WebhookDeliveryRepository = (*WebhookDeliveryRepo)(nil)
