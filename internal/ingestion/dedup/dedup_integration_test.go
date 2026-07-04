package dedup_test

import (
	"context"
	"testing"

	"github.com/google/uuid"

	"github.com/koriebruh/Jengine/internal/ingestion/connector"
	"github.com/koriebruh/Jengine/internal/ingestion/dedup"
	"github.com/koriebruh/Jengine/internal/ingestion/pipeline"
	"github.com/koriebruh/Jengine/internal/storage/postgres"
	"github.com/koriebruh/Jengine/internal/tenancy"
	"github.com/koriebruh/Jengine/internal/testutil"
)

// alwaysFalseBloom simulates a bloom filter that is completely non-
// functional (or absent) - always reports "definitely not seen." Used to
// prove the Postgres unique-constraint path alone is sufficient for
// correctness (plans/task/core/09 DoD).
type alwaysFalseBloom struct{}

func (alwaysFalseBloom) MayExist(ctx context.Context, tenantID uuid.UUID, key string) (bool, error) {
	return false, nil
}
func (alwaysFalseBloom) Add(ctx context.Context, tenantID uuid.UUID, key string) error { return nil }

func newTestRecord(tenantID, connectorID uuid.UUID, ref string) *pipeline.PipelineRecord {
	return &pipeline.PipelineRecord{
		Raw: connector.RawRecord{TenantID: tenantID, ConnectorID: connectorID, BatchID: uuid.New()},
		Normalized: pipeline.NormalizedFields{
			ExternalRef: ref,
		},
	}
}

func setupDedupStage(t *testing.T, bloom dedup.BloomFilter) (*dedup.DedupStage, uuid.UUID, uuid.UUID, context.Context) {
	t.Helper()

	db := testutil.StartPostgres(t)
	ctx := context.Background()

	tenantID := uuid.New()
	if _, err := db.Pool.Exec(ctx,
		`INSERT INTO tenants (id, name, isolation_tier, region, status) VALUES ($1, 'Acme', 'STANDARD', 'us-east', 'ACTIVE')`,
		tenantID,
	); err != nil {
		t.Fatalf("seed tenant failed: %v", err)
	}
	connectorID := uuid.New()
	if _, err := db.Pool.Exec(ctx,
		`INSERT INTO connectors (id, tenant_id, type, status) VALUES ($1, $2, 'test', 'ACTIVE')`,
		connectorID, tenantID,
	); err != nil {
		t.Fatalf("seed connector failed: %v", err)
	}

	appPool := testutil.AppRolePool(t, ctx, db.DSN)
	t.Cleanup(appPool.Close)

	txRepo := postgres.NewTransactionRepo()
	dedupRepo := postgres.NewIngestionDedupRepo()

	txRunner := func(ctx context.Context, tid uuid.UUID, fn func(ctx context.Context) error) error {
		return postgres.WithTx(tenancy.WithTenant(ctx, tenancy.TenantContext{TenantID: tid}), appPool, tid, fn)
	}

	stage := &dedup.DedupStage{
		TenantID: tenantID, ConnectorID: connectorID,
		Bloom: bloom, Transactions: txRepo, Dedup: dedupRepo, TxRunner: txRunner,
	}
	return stage, tenantID, connectorID, ctx
}

func TestDedupStage_SameRecordProcessedTwice_OnlyOneRowSurvives(t *testing.T) {
	if testing.Short() {
		t.Skip("requires Docker - skipped under -short; run make test-integration")
	}

	realBloom := &fakeInMemoryBloom{seen: map[string]bool{}}
	stage, tenantID, connectorID, ctx := setupDedupStage(t, realBloom)

	rec := newTestRecord(tenantID, connectorID, "REF-DUP-001")
	// Second call reuses the identical Raw.BatchID/ExternalRef so the
	// idempotency key is identical - simulating a genuine replay of the
	// same source record, not just parallel independent records.
	rec2 := newTestRecord(tenantID, connectorID, "REF-DUP-001")
	rec2.Raw.BatchID = rec.Raw.BatchID

	result1, err := stage.Process(ctx, rec)
	if err != nil {
		t.Fatalf("first Process failed: %v", err)
	}
	if result1 != pipeline.StageContinue {
		t.Fatalf("expected first processing to continue, got %v", result1)
	}
	if rec.IdempotencyKey == "" {
		t.Fatal("expected IdempotencyKey to be set after StageContinue")
	}

	result2, err := stage.Process(ctx, rec2)
	if err != nil {
		t.Fatalf("second Process failed: %v", err)
	}
	if result2 != pipeline.StageDrop {
		t.Fatalf("expected second (duplicate) processing to drop, got %v", result2)
	}
}

func TestDedupStage_CorrectEvenWithBloomFilterDisabled(t *testing.T) {
	if testing.Short() {
		t.Skip("requires Docker - skipped under -short; run make test-integration")
	}

	// alwaysFalseBloom always says "definitely not seen," so DedupStage
	// skips the ExistsByIdempotencyKey fast-confirm path entirely and
	// goes straight to TryInsert both times - proving the Postgres
	// UNIQUE constraint alone (not the bloom filter) is what makes the
	// second call correctly drop.
	stage, tenantID, connectorID, ctx := setupDedupStage(t, alwaysFalseBloom{})

	rec := newTestRecord(tenantID, connectorID, "REF-DUP-002")
	rec2 := newTestRecord(tenantID, connectorID, "REF-DUP-002")
	rec2.Raw.BatchID = rec.Raw.BatchID

	result1, err := stage.Process(ctx, rec)
	if err != nil {
		t.Fatalf("first Process failed: %v", err)
	}
	if result1 != pipeline.StageContinue {
		t.Fatalf("expected first processing to continue, got %v", result1)
	}

	result2, err := stage.Process(ctx, rec2)
	if err != nil {
		t.Fatalf("second Process failed: %v", err)
	}
	if result2 != pipeline.StageDrop {
		t.Fatalf("expected second processing to drop via the DB unique constraint alone (bloom filter reported false both times), got %v", result2)
	}
}

// fakeInMemoryBloom is a real (non-degenerate) in-memory bloom-like set,
// used where the test wants normal bloom-filter behavior (fast-path
// works) rather than alwaysFalseBloom's deliberately-broken stand-in.
type fakeInMemoryBloom struct {
	seen map[string]bool
}

func (b *fakeInMemoryBloom) MayExist(ctx context.Context, tenantID uuid.UUID, key string) (bool, error) {
	return b.seen[tenantID.String()+"|"+key], nil
}
func (b *fakeInMemoryBloom) Add(ctx context.Context, tenantID uuid.UUID, key string) error {
	b.seen[tenantID.String()+"|"+key] = true
	return nil
}
