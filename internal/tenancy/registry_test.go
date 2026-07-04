package tenancy_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/koriebruh/Jengine/internal/tenancy"
	"github.com/koriebruh/Jengine/internal/testutil"
)

func seedTenant(t *testing.T, ctx context.Context, db *testutil.TestDB) uuid.UUID {
	t.Helper()
	tenantID := uuid.New()
	_, err := db.Pool.Exec(ctx,
		`INSERT INTO tenants (id, name, isolation_tier, region, status) VALUES ($1, 'Acme', 'STANDARD', 'us-east', 'ACTIVE')`,
		tenantID,
	)
	if err != nil {
		t.Fatalf("seed tenant failed: %v", err)
	}
	return tenantID
}

func TestPostgresRegistryRepo(t *testing.T) {
	if testing.Short() {
		t.Skip("requires Docker - skipped under -short; run make test-integration")
	}

	db := testutil.StartPostgres(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	repo := tenancy.NewPostgresRegistryRepo(db.Pool)
	tenantID := seedTenant(t, ctx, db)

	t.Run("GetTenant", func(t *testing.T) {
		got, err := repo.GetTenant(ctx, tenantID)
		if err != nil {
			t.Fatalf("GetTenant failed: %v", err)
		}
		if got.Name != "Acme" || got.IsolationTier != tenancy.IsolationTierStandard {
			t.Errorf("got %+v, want name=Acme isolation_tier=STANDARD", got)
		}
	})

	t.Run("GetTenant not found", func(t *testing.T) {
		_, err := repo.GetTenant(ctx, uuid.New())
		if err != tenancy.ErrNotFound {
			t.Errorf("expected ErrNotFound, got %v", err)
		}
	})

	t.Run("GetTenantByAPIKeyHash", func(t *testing.T) {
		_, err := db.Pool.Exec(ctx,
			`INSERT INTO tenant_api_keys (tenant_id, key_hash) VALUES ($1, 'hash-abc123')`,
			tenantID,
		)
		if err != nil {
			t.Fatalf("seed api key failed: %v", err)
		}

		got, err := repo.GetTenantByAPIKeyHash(ctx, "hash-abc123")
		if err != nil {
			t.Fatalf("GetTenantByAPIKeyHash failed: %v", err)
		}
		if got.ID != tenantID {
			t.Errorf("got tenant %s, want %s", got.ID, tenantID)
		}
	})

	t.Run("GetTenantByAPIKeyHash ignores revoked keys", func(t *testing.T) {
		_, err := db.Pool.Exec(ctx,
			`INSERT INTO tenant_api_keys (tenant_id, key_hash, revoked_at) VALUES ($1, 'hash-revoked', now())`,
			tenantID,
		)
		if err != nil {
			t.Fatalf("seed revoked api key failed: %v", err)
		}
		_, err = repo.GetTenantByAPIKeyHash(ctx, "hash-revoked")
		if err != tenancy.ErrNotFound {
			t.Errorf("expected ErrNotFound for a revoked key, got %v", err)
		}
	})

	t.Run("GetIsolationConfig", func(t *testing.T) {
		_, err := db.Pool.Exec(ctx,
			`INSERT INTO tenant_isolation_config (tenant_id, shard_id) VALUES ($1, 'shard-0')`,
			tenantID,
		)
		if err != nil {
			t.Fatalf("seed isolation config failed: %v", err)
		}
		got, err := repo.GetIsolationConfig(ctx, tenantID)
		if err != nil {
			t.Fatalf("GetIsolationConfig failed: %v", err)
		}
		if got.ShardID != "shard-0" {
			t.Errorf("got shard_id %q, want %q", got.ShardID, "shard-0")
		}
	})

	t.Run("GetQuota", func(t *testing.T) {
		_, err := db.Pool.Exec(ctx,
			`INSERT INTO tenant_quota (tenant_id, ingestion_rate_limit, matching_job_concurrency, storage_quota_bytes) VALUES ($1, 1000, 4, 1073741824)`,
			tenantID,
		)
		if err != nil {
			t.Fatalf("seed quota failed: %v", err)
		}
		got, err := repo.GetQuota(ctx, tenantID)
		if err != nil {
			t.Fatalf("GetQuota failed: %v", err)
		}
		if got.IngestionRateLimit != 1000 || got.MatchingJobConcurrency != 4 || got.StorageQuotaBytes != 1073741824 {
			t.Errorf("got %+v, unexpected values", got)
		}
	})

	t.Run("IsFeatureEnabled", func(t *testing.T) {
		_, err := db.Pool.Exec(ctx,
			`INSERT INTO tenant_feature_flags (tenant_id, flag_key, enabled) VALUES ($1, 'streaming_recon', true)`,
			tenantID,
		)
		if err != nil {
			t.Fatalf("seed feature flag failed: %v", err)
		}
		enabled, err := repo.IsFeatureEnabled(ctx, tenantID, "streaming_recon")
		if err != nil {
			t.Fatalf("IsFeatureEnabled failed: %v", err)
		}
		if !enabled {
			t.Error("expected streaming_recon to be enabled")
		}

		// Unset flag defaults to false, not an error.
		enabled, err = repo.IsFeatureEnabled(ctx, tenantID, "never_set")
		if err != nil {
			t.Fatalf("IsFeatureEnabled (unset flag) failed: %v", err)
		}
		if enabled {
			t.Error("expected an unset flag to default to false")
		}
	})
}

func TestCachedRegistryRepo(t *testing.T) {
	if testing.Short() {
		t.Skip("requires Docker - skipped under -short; run make test-integration")
	}

	db := testutil.StartPostgres(t)
	rdb := testutil.StartRedis(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	inner := tenancy.NewPostgresRegistryRepo(db.Pool)
	cached := tenancy.NewCachedRegistryRepo(inner, rdb.Client)

	tenantID := seedTenant(t, ctx, db)
	_, err := db.Pool.Exec(ctx,
		`INSERT INTO tenant_api_keys (tenant_id, key_hash) VALUES ($1, 'hash-cached')`,
		tenantID,
	)
	if err != nil {
		t.Fatalf("seed api key failed: %v", err)
	}
	_, err = db.Pool.Exec(ctx,
		`INSERT INTO tenant_isolation_config (tenant_id, shard_id) VALUES ($1, 'shard-0')`,
		tenantID,
	)
	if err != nil {
		t.Fatalf("seed isolation config failed: %v", err)
	}

	t.Run("GetTenantByAPIKeyHash is cache-consistent across calls", func(t *testing.T) {
		first, err := cached.GetTenantByAPIKeyHash(ctx, "hash-cached")
		if err != nil {
			t.Fatalf("first call failed: %v", err)
		}
		// Delete the underlying row - a cached second call must still
		// succeed (proving it actually served from cache, not the DB).
		if _, err := db.Pool.Exec(ctx, `DELETE FROM tenant_api_keys WHERE key_hash = 'hash-cached'`); err != nil {
			t.Fatalf("delete failed: %v", err)
		}

		second, err := cached.GetTenantByAPIKeyHash(ctx, "hash-cached")
		if err != nil {
			t.Fatalf("second (cached) call failed: %v", err)
		}
		if second.ID != first.ID {
			t.Errorf("cached result mismatch: got %s, want %s", second.ID, first.ID)
		}
	})

	t.Run("GetIsolationConfig is cache-consistent across calls", func(t *testing.T) {
		first, err := cached.GetIsolationConfig(ctx, tenantID)
		if err != nil {
			t.Fatalf("first call failed: %v", err)
		}
		if _, err := db.Pool.Exec(ctx, `DELETE FROM tenant_isolation_config WHERE tenant_id = $1`, tenantID); err != nil {
			t.Fatalf("delete failed: %v", err)
		}

		second, err := cached.GetIsolationConfig(ctx, tenantID)
		if err != nil {
			t.Fatalf("second (cached) call failed: %v", err)
		}
		if second.ShardID != first.ShardID {
			t.Errorf("cached result mismatch: got %q, want %q", second.ShardID, first.ShardID)
		}
	})
}
