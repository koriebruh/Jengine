package postgres

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/koriebruh/Jengine/internal/ingestion"
	"github.com/koriebruh/Jengine/internal/ingestion/pipeline"
)

// PersistFunc does the actual domain-specific persistence for one
// PipelineRecord (e.g. writing a Transaction row via
// internal/storage/postgres's TransactionRepo - plans/task/core/07-09
// supply the real implementation once field mapping/normalization exist;
// this task's own tests supply a trivial one) and returns the topic/key/
// payload to publish via the transactional outbox.
type PersistFunc func(ctx context.Context, rec *pipeline.PipelineRecord) (topic, key string, eventPayload []byte, err error)

// PersistEmitStage is stage 8 (Persist + Emit Event) of
// plans/docs/02-data-ingestion.md §3.2: within one DB transaction, calls
// Persist (the domain-specific write) and then writes the corresponding
// outbox row via Outbox.Write - the transactional-outbox pattern
// (plans/docs/06-streaming-architecture.md §7.3), never a dual write
// where the DB commit could succeed while a direct Kafka publish fails
// (or vice versa). This is deliberately generic over WHAT gets
// persisted, since the real domain mapping depends on tasks 07-09; this
// task owns making the persist+outbox-write atomic, not the mapping
// logic itself (see plans/task/core/06 Non-Goals).
type PersistEmitStage struct {
	Pool     *pgxpool.Pool
	TenantID uuid.UUID
	Outbox   ingestion.OutboxWriter
	Persist  PersistFunc
}

// tenantcheck:exempt - signature fixed by pipeline.Stage, no room for a
// context or tenantID parameter on a plain name getter.
func (s *PersistEmitStage) Name() string { return "persist_emit" }

// tenantcheck:exempt - signature fixed by pipeline.Stage; tenant scoping
// happens via s.TenantID (set at construction) passed into WithTx below,
// not a method parameter - the interface this satisfies has no room for
// one.
func (s *PersistEmitStage) Process(ctx context.Context, rec *pipeline.PipelineRecord) (pipeline.StageResult, error) {
	err := WithTx(ctx, s.Pool, s.TenantID, func(ctx context.Context) error {
		topic, key, payload, err := s.Persist(ctx, rec)
		if err != nil {
			return fmt.Errorf("persist: %w", err)
		}
		if err := s.Outbox.Write(ctx, s.TenantID, topic, key, payload); err != nil {
			return fmt.Errorf("outbox write: %w", err)
		}
		return nil
	})
	if err != nil {
		return pipeline.StageQuarantine, err
	}
	return pipeline.StageContinue, nil
}

var _ pipeline.Stage = (*PersistEmitStage)(nil)
