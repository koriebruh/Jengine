package batch_test

import (
	"context"
	"testing"
	"time"

	"github.com/koriebruh/Jengine/internal/matching/batch"
	"github.com/koriebruh/Jengine/internal/testutil"
)

func TestEnsureRiverSchema(t *testing.T) {
	if testing.Short() {
		t.Skip("requires Docker - skipped under -short; run make test-integration")
	}

	db := testutil.StartPostgres(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := batch.EnsureRiverSchema(ctx, db.Pool); err != nil {
		t.Fatalf("EnsureRiverSchema failed: %v", err)
	}

	// Idempotent - a second call must not error.
	if err := batch.EnsureRiverSchema(ctx, db.Pool); err != nil {
		t.Fatalf("second EnsureRiverSchema call failed: %v", err)
	}

	var tableCount int
	if err := db.Pool.QueryRow(ctx,
		`SELECT count(*) FROM information_schema.tables WHERE table_name = 'river_job'`,
	).Scan(&tableCount); err != nil {
		t.Fatalf("check river_job table failed: %v", err)
	}
	if tableCount != 1 {
		t.Fatalf("expected river_job table to exist after EnsureRiverSchema, got count=%d", tableCount)
	}
}
