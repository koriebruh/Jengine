package postgres

import (
	"context"
	"fmt"

	"github.com/google/uuid"

	"github.com/koriebruh/Jengine/internal/domain"
)

// IngestionDedupRepo implements domain.IngestionDedupRepository.
type IngestionDedupRepo struct{}

func NewIngestionDedupRepo() *IngestionDedupRepo {
	return &IngestionDedupRepo{}
}

func (r *IngestionDedupRepo) TryInsert(ctx context.Context, tenantID uuid.UUID, idempotencyKey string, connectorID uuid.UUID, batchID string) (bool, error) {
	tx, err := requireTx(ctx, tenantID)
	if err != nil {
		return false, err
	}

	tag, err := tx.Exec(ctx,
		`INSERT INTO ingestion_dedup (tenant_id, idempotency_key, source_connector_id, ingestion_batch_id)
		 VALUES ($1, $2, $3, $4)
		 ON CONFLICT (tenant_id, idempotency_key) DO NOTHING`,
		tenantID, idempotencyKey, connectorID, batchID,
	)
	if err != nil {
		return false, fmt.Errorf("postgres: IngestionDedupRepo.TryInsert: %w", err)
	}
	return tag.RowsAffected() == 1, nil
}

var _ domain.IngestionDedupRepository = (*IngestionDedupRepo)(nil)
