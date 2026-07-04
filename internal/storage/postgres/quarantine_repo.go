package postgres

import (
	"context"
	"fmt"

	"github.com/google/uuid"

	"github.com/koriebruh/Jengine/internal/ingestion"
)

// QuarantineRepo implements ingestion.QuarantineSink.
type QuarantineRepo struct{}

func NewQuarantineRepo() *QuarantineRepo {
	return &QuarantineRepo{}
}

func (r *QuarantineRepo) Quarantine(ctx context.Context, tenantID, connectorID uuid.UUID, stage, reason string, payload []byte) error {
	tx, err := requireTx(ctx, tenantID)
	if err != nil {
		return err
	}

	var connectorIDArg any
	if connectorID != uuid.Nil {
		connectorIDArg = connectorID
	}

	_, err = tx.Exec(ctx,
		`INSERT INTO quarantine_entries (tenant_id, connector_id, stage, reason, raw_payload)
		 VALUES ($1, $2, $3, $4, $5)`,
		tenantID, connectorIDArg, stage, reason, payload,
	)
	if err != nil {
		return fmt.Errorf("postgres: QuarantineRepo.Quarantine: %w", err)
	}
	return nil
}

func (r *QuarantineRepo) List(ctx context.Context, tenantID uuid.UUID, connectorID uuid.UUID) ([]ingestion.QuarantineEntry, error) {
	tx, err := requireTx(ctx, tenantID)
	if err != nil {
		return nil, err
	}

	rows, err := tx.Query(ctx,
		`SELECT id, tenant_id, connector_id, stage, reason, raw_payload, occurred_at
		 FROM quarantine_entries WHERE connector_id = $1 ORDER BY occurred_at DESC`,
		connectorID,
	)
	if err != nil {
		return nil, fmt.Errorf("postgres: QuarantineRepo.List: %w", err)
	}
	defer rows.Close()

	var entries []ingestion.QuarantineEntry
	for rows.Next() {
		var e ingestion.QuarantineEntry
		if err := rows.Scan(&e.ID, &e.TenantID, &e.ConnectorID, &e.Stage, &e.Reason, &e.RawPayload, &e.OccurredAt); err != nil {
			return nil, fmt.Errorf("postgres: QuarantineRepo.List: scan: %w", err)
		}
		entries = append(entries, e)
	}
	return entries, rows.Err()
}

var _ ingestion.QuarantineSink = (*QuarantineRepo)(nil)
