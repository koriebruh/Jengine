package testutil

import (
	"context"
	"testing"
	"time"
)

// Minimal smoke tests proving StartPostgres/StartRedis work end-to-end,
// per plans/task/core/17's Definition of Done. Skipped under `-short`
// (used by `make test-unit`) since these need a real Docker daemon -
// run via `make test-integration` instead.

func TestStartPostgres_Smoke(t *testing.T) {
	if testing.Short() {
		t.Skip("requires Docker - skipped under -short (make test-unit); run make test-integration")
	}

	db := StartPostgres(t)
	if db.Pool == nil {
		t.Fatal("expected a non-nil pgx pool")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var one int
	if err := db.Pool.QueryRow(ctx, "SELECT 1").Scan(&one); err != nil {
		t.Fatalf("expected to query the live container, got: %v", err)
	}
	if one != 1 {
		t.Fatalf("expected 1, got %d", one)
	}
}

func TestStartRedis_Smoke(t *testing.T) {
	if testing.Short() {
		t.Skip("requires Docker - skipped under -short (make test-unit); run make test-integration")
	}

	rdb := StartRedis(t)
	if rdb.Client == nil {
		t.Fatal("expected a non-nil redis client")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := rdb.Client.Set(ctx, "testutil-smoke", "ok", 0).Err(); err != nil {
		t.Fatalf("expected to write to the live container, got: %v", err)
	}
	got, err := rdb.Client.Get(ctx, "testutil-smoke").Result()
	if err != nil {
		t.Fatalf("expected to read back from the live container, got: %v", err)
	}
	if got != "ok" {
		t.Fatalf("expected %q, got %q", "ok", got)
	}
}
