package dedup_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/google/uuid"

	"github.com/koriebruh/Jengine/internal/ingestion/dedup"
	"github.com/koriebruh/Jengine/internal/testutil"
)

func TestRedisBloomFilter_AddAndMayExistRoundTrip(t *testing.T) {
	if testing.Short() {
		t.Skip("requires Docker - skipped under -short; run make test-integration")
	}

	rdb := testutil.StartRedis(t)
	ctx := context.Background()
	tenantID := uuid.New()

	bf := dedup.NewRedisBloomFilter(rdb.Client, "test:bloom", 1000, 0.01)

	exists, err := bf.MayExist(ctx, tenantID, "never-added-key")
	if err != nil {
		t.Fatalf("MayExist failed: %v", err)
	}
	if exists {
		t.Error("expected a never-added key to definitely not exist")
	}

	if err := bf.Add(ctx, tenantID, "my-key"); err != nil {
		t.Fatalf("Add failed: %v", err)
	}

	exists, err = bf.MayExist(ctx, tenantID, "my-key")
	if err != nil {
		t.Fatalf("MayExist failed: %v", err)
	}
	if !exists {
		t.Error("expected an added key to be reported as maybe-existing")
	}
}

func TestRedisBloomFilter_TenantsAreIsolated(t *testing.T) {
	if testing.Short() {
		t.Skip("requires Docker - skipped under -short; run make test-integration")
	}

	rdb := testutil.StartRedis(t)
	ctx := context.Background()
	tenantA, tenantB := uuid.New(), uuid.New()

	bf := dedup.NewRedisBloomFilter(rdb.Client, "test:bloom", 1000, 0.01)

	if err := bf.Add(ctx, tenantA, "shared-key"); err != nil {
		t.Fatalf("Add failed: %v", err)
	}

	existsB, err := bf.MayExist(ctx, tenantB, "shared-key")
	if err != nil {
		t.Fatalf("MayExist failed: %v", err)
	}
	if existsB {
		t.Error("expected tenant B's bloom filter to be unaffected by tenant A's Add")
	}
}

// TestRedisBloomFilter_FalsePositiveRateNearTarget seeds N known keys,
// then checks M unknown keys, asserting the observed false-positive rate
// stays within a reasonable tolerance of the configured target
// (plans/task/core/09 DoD).
func TestRedisBloomFilter_FalsePositiveRateNearTarget(t *testing.T) {
	if testing.Short() {
		t.Skip("requires Docker - skipped under -short; run make test-integration")
	}

	rdb := testutil.StartRedis(t)
	ctx := context.Background()
	tenantID := uuid.New()

	const n = 2000
	const targetFPRate = 0.01
	bf := dedup.NewRedisBloomFilter(rdb.Client, "test:bloom:fp", n, targetFPRate)

	for i := 0; i < n; i++ {
		if err := bf.Add(ctx, tenantID, fmt.Sprintf("known-key-%d", i)); err != nil {
			t.Fatalf("Add failed: %v", err)
		}
	}

	const m = 2000
	falsePositives := 0
	for i := 0; i < m; i++ {
		exists, err := bf.MayExist(ctx, tenantID, fmt.Sprintf("unknown-key-%d", i))
		if err != nil {
			t.Fatalf("MayExist failed: %v", err)
		}
		if exists {
			falsePositives++
		}
	}

	observedRate := float64(falsePositives) / float64(m)
	// Generous tolerance (5x target) - this is a statistical property,
	// not an exact guarantee, and we're testing "roughly matches," not
	// pinning an exact number that could flake.
	maxAcceptable := targetFPRate * 5
	if observedRate > maxAcceptable {
		t.Errorf("observed false-positive rate %.4f exceeds tolerance %.4f (target was %.4f)", observedRate, maxAcceptable, targetFPRate)
	}
	t.Logf("observed false-positive rate: %.4f (target: %.4f)", observedRate, targetFPRate)
}
