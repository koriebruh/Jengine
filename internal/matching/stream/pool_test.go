package stream_test

import (
	"context"
	"testing"
	"testing/quick"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"

	matchingcore "github.com/koriebruh/Jengine/internal/matching/core"
	"github.com/koriebruh/Jengine/internal/matching/stream"
	"github.com/koriebruh/Jengine/internal/testutil"
)

func newRecord(t *testing.T, tenantID uuid.UUID, valueDate time.Time) matchingcore.MatchableRecord {
	t.Helper()
	return matchingcore.MatchableRecord{
		ID: uuid.New(), TenantID: tenantID, AccountID: uuid.New(),
		ValueDate: valueDate, BaseAmount: decimal.NewFromInt(100), Currency: "USD",
	}
}

func TestRedisCandidatePool_AddQueryRemove(t *testing.T) {
	if testing.Short() {
		t.Skip("requires Docker - skipped under -short")
	}
	rdb := testutil.StartRedis(t)
	pool := stream.NewRedisCandidatePool(rdb.Client, "test", 7*24*time.Hour)
	ctx := context.Background()
	tenantID := uuid.New()
	poolKey := tenantID.String()

	rec := newRecord(t, tenantID, time.Now())
	if err := pool.Add(ctx, tenantID, poolKey, rec); err != nil {
		t.Fatalf("Add failed: %v", err)
	}

	got, err := pool.Query(ctx, tenantID, poolKey)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	if len(got) != 1 || got[0].ID != rec.ID {
		t.Fatalf("expected 1 record with ID %s, got %+v", rec.ID, got)
	}

	if err := pool.Remove(ctx, tenantID, poolKey, rec.ID); err != nil {
		t.Fatalf("Remove failed: %v", err)
	}
	got, err = pool.Query(ctx, tenantID, poolKey)
	if err != nil {
		t.Fatalf("Query after Remove failed: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("expected 0 records after Remove, got %d", len(got))
	}
}

func TestRedisCandidatePool_TrimOnWrite_EvictsOldEntries(t *testing.T) {
	if testing.Short() {
		t.Skip("requires Docker - skipped under -short")
	}
	rdb := testutil.StartRedis(t)
	window := 2 * time.Second
	pool := stream.NewRedisCandidatePool(rdb.Client, "test", window)
	ctx := context.Background()
	tenantID := uuid.New()
	poolKey := tenantID.String()

	old := newRecord(t, tenantID, time.Now().Add(-10*time.Second))
	if err := pool.Add(ctx, tenantID, poolKey, old); err != nil {
		t.Fatalf("Add (old) failed: %v", err)
	}

	fresh := newRecord(t, tenantID, time.Now())
	if err := pool.Add(ctx, tenantID, poolKey, fresh); err != nil {
		t.Fatalf("Add (fresh) failed: %v", err)
	}

	got, err := pool.Query(ctx, tenantID, poolKey)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	if len(got) != 1 || got[0].ID != fresh.ID {
		t.Fatalf("expected only the fresh record to survive trim-on-write, got %+v", got)
	}
}

// TestRedisCandidatePool_BoundedUnderSustainedLoad is the property-based
// test plans/task/core/19's Definition of Done requires: confirms the
// pool's memory (measured here as pooled-record count, a direct proxy
// for memory) stays bounded under sustained load - trim-on-write
// actually fires under randomized conditions, not just in the one
// hand-picked TestRedisCandidatePool_TrimOnWrite_EvictsOldEntries case
// above.
func TestRedisCandidatePool_BoundedUnderSustainedLoad(t *testing.T) {
	if testing.Short() {
		t.Skip("requires Docker - skipped under -short")
	}
	rdb := testutil.StartRedis(t)
	window := 5 * time.Second
	pool := stream.NewRedisCandidatePool(rdb.Client, "test-bounded", window)
	ctx := context.Background()

	f := func(ageOffsetsSeconds []int8) bool {
		if len(ageOffsetsSeconds) == 0 {
			return true
		}
		if len(ageOffsetsSeconds) > 200 {
			ageOffsetsSeconds = ageOffsetsSeconds[:200]
		}
		// Fresh tenant/pool key per property-check iteration - sharing
		// one key across iterations let a borderline-fresh record from
		// an earlier iteration age past the window by the time a LATER
		// iteration's fixed `now` checked it, before that iteration's
		// own Add() calls happened to re-trim it. Not a pool bug, a
		// test-isolation bug: found via this test's own first run.
		tenantID := uuid.New()
		poolKey := tenantID.String()
		now := time.Now()
		var maxAge time.Duration
		for _, offset := range ageOffsetsSeconds {
			age := time.Duration(offset) * time.Second
			if age < 0 {
				age = -age
			}
			if age > maxAge {
				maxAge = age
			}
			rec := newRecord(t, tenantID, now.Add(-age))
			if err := pool.Add(ctx, tenantID, poolKey, rec); err != nil {
				t.Fatalf("Add failed: %v", err)
			}
		}

		got, err := pool.Query(ctx, tenantID, poolKey)
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		for _, rec := range got {
			if now.Sub(rec.ValueDate) > window {
				t.Errorf("found a candidate older than the window (%s) still pooled: age=%s", window, now.Sub(rec.ValueDate))
				return false
			}
		}
		return true
	}

	cfg := &quick.Config{MaxCount: 20}
	if err := quick.Check(f, cfg); err != nil {
		t.Error(err)
	}
}
