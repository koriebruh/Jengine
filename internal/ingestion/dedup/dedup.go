package dedup

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/google/uuid"

	"github.com/koriebruh/Jengine/internal/ingestion/pipeline"
)

// TransactionExistsChecker is the surface DedupStage needs for the
// authoritative fallback check - internal/storage/postgres.TransactionRepo
// satisfies it structurally (domain.TransactionRepository.ExistsByIdempotencyKey).
type TransactionExistsChecker interface {
	ExistsByIdempotencyKey(ctx context.Context, tenantID uuid.UUID, key string) (bool, error)
}

// DedupRepo is the surface DedupStage needs for the authoritative guard -
// internal/storage/postgres.IngestionDedupRepo satisfies it structurally.
type DedupRepo interface {
	TryInsert(ctx context.Context, tenantID uuid.UUID, idempotencyKey string, connectorID uuid.UUID, batchID string) (bool, error)
}

// TxRunner wraps fn in a transaction scoped to tenantID - same shape and
// rationale as every other Stage in this pipeline needing DB access (see
// mapping.TxRunner, connector/csvupload.TxRunner,
// connector/sftp.TxRunner): Process is called directly by
// pipeline.Pipeline.Run outside any ambient transaction.
type TxRunner func(ctx context.Context, tenantID uuid.UUID, fn func(ctx context.Context) error) error

// NaturalKeyFunc extracts a record's natural key (or a stable fallback
// hash) for idempotency-key computation - format-specific, since only the
// caller knows which field (if any) is a genuine natural key for a given
// source format (plans/task/core/09 Implementation Notes).
type NaturalKeyFunc func(rec *pipeline.PipelineRecord) string

// DefaultNaturalKeyFunc prefers rec.Normalized.ExternalRef (the field
// mapping stage's "transaction.reference" target, e.g. MT940's
// extract_regex("REF:(\S+)") result) when present, falling back to a
// RecordHash of the normalized fields' stable string form.
func DefaultNaturalKeyFunc(rec *pipeline.PipelineRecord) string {
	if rec.Normalized.ExternalRef != "" {
		return rec.Normalized.ExternalRef
	}
	return RecordHash(map[string]string{
		"amount":       rec.Normalized.Amount.String(),
		"currency":     rec.Normalized.Currency,
		"value_date":   rec.Normalized.ValueDate.Format("2006-01-02"),
		"description":  rec.Normalized.Description,
		"counterparty": rec.Normalized.CounterpartyRef,
	})
}

// DedupStage implements pipeline stage 6 (Dedup/Idempotency,
// plans/task/core/09): computes the idempotency key, checks the bloom
// filter fast path, falls through to the authoritative
// ingestion_dedup/Transaction check, and drops (never silently) any
// record already processed.
type DedupStage struct {
	TenantID     uuid.UUID
	ConnectorID  uuid.UUID
	Bloom        BloomFilter
	Transactions TransactionExistsChecker
	Dedup        DedupRepo
	TxRunner     TxRunner
	NaturalKey   NaturalKeyFunc // defaults to DefaultNaturalKeyFunc if nil
}

func (s *DedupStage) Name() string { return "dedup" }

func (s *DedupStage) Process(ctx context.Context, rec *pipeline.PipelineRecord) (pipeline.StageResult, error) {
	naturalKeyFn := s.NaturalKey
	if naturalKeyFn == nil {
		naturalKeyFn = DefaultNaturalKeyFunc
	}
	key := ComputeIdempotencyKey(s.TenantID, s.ConnectorID, naturalKeyFn(rec), rec.Raw.BatchID)

	// The bloom filter is a fast-path optimization only - a lookup error
	// here must never block ingestion; fail open to the authoritative
	// check rather than erroring out (plans/task/core/09 Common Pitfalls:
	// correctness must never depend on the bloom filter being present).
	maybeExists, bloomErr := s.Bloom.MayExist(ctx, s.TenantID, key)
	if bloomErr != nil {
		maybeExists = true
	}

	var dropped bool
	err := s.TxRunner(ctx, s.TenantID, func(ctx context.Context) error {
		if maybeExists {
			exists, err := s.Transactions.ExistsByIdempotencyKey(ctx, s.TenantID, key)
			if err != nil {
				return fmt.Errorf("check existing transaction: %w", err)
			}
			if exists {
				dropped = true
				return nil
			}
		}

		// The UNIQUE (tenant_id, idempotency_key) constraint is the
		// actual race-safe guard - this insert is what makes concurrent
		// processing of the "same" record safe, not the check above
		// (plans/task/core/09 Common Pitfalls: never rely on
		// check-then-insert without the DB constraint backing it).
		inserted, err := s.Dedup.TryInsert(ctx, s.TenantID, key, s.ConnectorID, rec.Raw.BatchID.String())
		if err != nil {
			return fmt.Errorf("try insert dedup entry: %w", err)
		}
		if !inserted {
			dropped = true
		}
		return nil
	})
	if err != nil {
		return pipeline.StageQuarantine, fmt.Errorf("dedup: %w", err)
	}

	if dropped {
		// StageDrop must be logged, never silent
		// (plans/task/core/06 Implementation Notes; plans/task/core/09
		// Common Pitfalls) - this is the concrete case that requirement
		// exists for.
		slog.InfoContext(ctx, "dedup: dropping duplicate record",
			"tenant_id", s.TenantID, "connector_id", s.ConnectorID,
			"idempotency_key", key, "reason", "duplicate")
		return pipeline.StageDrop, nil
	}

	// Best-effort bloom filter write - a failure here doesn't affect
	// correctness (the authoritative check above already succeeded), it
	// only means a future record's fast path is slightly less effective.
	_ = s.Bloom.Add(ctx, s.TenantID, key)

	rec.IdempotencyKey = key
	return pipeline.StageContinue, nil
}

var _ pipeline.Stage = (*DedupStage)(nil)
