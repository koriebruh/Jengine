package tenancy_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/koriebruh/Jengine/internal/tenancy"
	"github.com/koriebruh/Jengine/internal/testutil"
)

func TestQuotaLimiter_Allow(t *testing.T) {
	if testing.Short() {
		t.Skip("requires Docker - skipped under -short; run make test-integration")
	}

	db := testutil.StartPostgres(t)
	rdb := testutil.StartRedis(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	tenantID := seedTenant(t, ctx, db)
	// A small rate (2/sec) so the test can exhaust the bucket quickly and
	// deterministically without needing to wait a long time.
	_, err := db.Pool.Exec(ctx,
		`INSERT INTO tenant_quota (tenant_id, ingestion_rate_limit, matching_job_concurrency, storage_quota_bytes) VALUES ($1, 2, 1, 0)`,
		tenantID,
	)
	if err != nil {
		t.Fatalf("seed quota failed: %v", err)
	}

	registry := tenancy.NewPostgresRegistryRepo(db.Pool)
	limiter := tenancy.NewQuotaLimiter(rdb.Client, registry)

	allowedCount := 0
	var lastRetryAfter time.Duration
	var sawDenied bool
	for i := 0; i < 5; i++ {
		allowed, retryAfter, err := limiter.Allow(ctx, tenantID, "ingestion")
		if err != nil {
			t.Fatalf("Allow call %d failed: %v", i, err)
		}
		if allowed {
			allowedCount++
		} else {
			sawDenied = true
			lastRetryAfter = retryAfter
		}
	}

	if allowedCount == 0 {
		t.Error("expected at least the first request to be allowed (bucket starts full)")
	}
	if allowedCount >= 5 {
		t.Error("expected rapid requests to eventually exceed the rate limit (2/sec), but all 5 were allowed")
	}
	if !sawDenied {
		t.Fatal("expected at least one denied request past the limit")
	}
	if lastRetryAfter <= 0 || lastRetryAfter > 2*time.Second {
		t.Errorf("expected a sane retryAfter (>0 and <=2s for a 2/sec limit), got %v", lastRetryAfter)
	}
}

func TestQuotaLimiter_HTTPMiddleware_TooManyRequests(t *testing.T) {
	if testing.Short() {
		t.Skip("requires Docker - skipped under -short; run make test-integration")
	}

	db := testutil.StartPostgres(t)
	rdb := testutil.StartRedis(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	tenantID := seedTenant(t, ctx, db)
	_, err := db.Pool.Exec(ctx,
		`INSERT INTO tenant_quota (tenant_id, ingestion_rate_limit, matching_job_concurrency, storage_quota_bytes) VALUES ($1, 1, 1, 0)`,
		tenantID,
	)
	if err != nil {
		t.Fatalf("seed quota failed: %v", err)
	}

	registry := tenancy.NewPostgresRegistryRepo(db.Pool)
	limiter := tenancy.NewQuotaLimiter(rdb.Client, registry)

	handlerCalls := 0
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handlerCalls++
		w.WriteHeader(http.StatusOK)
	})
	wrapped := limiter.HTTPMiddleware("ingestion", inner)

	tc := tenancy.TenantContext{TenantID: tenantID}
	newRequest := func() *http.Request {
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		return req.WithContext(tenancy.WithTenant(req.Context(), tc))
	}

	// First request within a 1/sec bucket should pass through.
	rec1 := httptest.NewRecorder()
	wrapped.ServeHTTP(rec1, newRequest())
	if rec1.Code != http.StatusOK {
		t.Fatalf("expected first request to pass (200), got %d", rec1.Code)
	}

	// Immediate second request should be rate-limited.
	rec2 := httptest.NewRecorder()
	wrapped.ServeHTTP(rec2, newRequest())
	if rec2.Code != http.StatusTooManyRequests {
		t.Fatalf("expected second immediate request to be rate-limited (429), got %d", rec2.Code)
	}
	if rec2.Header().Get("Retry-After") == "" {
		t.Error("expected a Retry-After header on a 429 response")
	}

	if handlerCalls != 1 {
		t.Errorf("expected the inner handler to run exactly once, ran %d times", handlerCalls)
	}
}
