package tenancy

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
)

// ErrNotFound is returned by RegistryRepo methods when the requested
// tenant/config/key doesn't exist.
var ErrNotFound = errors.New("tenancy: not found")

// Tenant mirrors the tenants table (plans/docs/03-canonical-data-model.md
// §4.1, plans/task/core/03).
type Tenant struct {
	ID            uuid.UUID
	Name          string
	IsolationTier IsolationTier
	Region        string
	Status        string
	CreatedAt     time.Time
}

// IsolationConfig mirrors tenant_isolation_config.
type IsolationConfig struct {
	TenantID   uuid.UUID
	ShardID    string
	SchemaName string
	ClusterRef string
}

// Quota mirrors tenant_quota.
type Quota struct {
	TenantID               uuid.UUID
	IngestionRateLimit     int
	MatchingJobConcurrency int
	StorageQuotaBytes      int64
}

// RegistryRepo reads the Tenant Registry (tenants, tenant_settings,
// tenant_isolation_config, tenant_quota, tenant_feature_flags -
// plans/task/core/03). This is the one deliberate, documented exception
// to "every repository query takes an explicit tenantID": it operates on
// the unsharded Tenant Registry DB, not per-tenant OLTP data, and its
// whole job is looking tenants UP (by ID or API key), so it cannot itself
// take a "current tenant" parameter. Do not "fix" this into taking one -
// see plans/task/core/04 Common Pitfalls. The tenancy lint analyzer
// (plans/task/core/04, internal/platform/lint/tenantcheck) allowlists this
// file for exactly this reason.
type RegistryRepo interface {
	GetTenant(ctx context.Context, tenantID uuid.UUID) (Tenant, error)
	GetTenantByAPIKeyHash(ctx context.Context, apiKeyHash string) (Tenant, error)
	GetIsolationConfig(ctx context.Context, tenantID uuid.UUID) (IsolationConfig, error)
	GetQuota(ctx context.Context, tenantID uuid.UUID) (Quota, error)
	IsFeatureEnabled(ctx context.Context, tenantID uuid.UUID, flag string) (bool, error)
}

// PostgresRegistryRepo is the uncached, direct-to-Postgres implementation.
// Wrap it with CachedRegistryRepo for the Redis-cached lookups
// plans/docs/01-multi-tenancy.md §2.2 calls for.
type PostgresRegistryRepo struct {
	pool *pgxpool.Pool
}

func NewPostgresRegistryRepo(pool *pgxpool.Pool) *PostgresRegistryRepo {
	return &PostgresRegistryRepo{pool: pool}
}

func (r *PostgresRegistryRepo) GetTenant(ctx context.Context, tenantID uuid.UUID) (Tenant, error) {
	var t Tenant
	var isolationTier string
	err := r.pool.QueryRow(ctx,
		`SELECT id, name, isolation_tier, region, status, created_at FROM tenants WHERE id = $1`,
		tenantID,
	).Scan(&t.ID, &t.Name, &isolationTier, &t.Region, &t.Status, &t.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return Tenant{}, ErrNotFound
	}
	if err != nil {
		return Tenant{}, fmt.Errorf("tenancy: GetTenant: %w", err)
	}
	t.IsolationTier = IsolationTier(isolationTier)
	return t, nil
}

func (r *PostgresRegistryRepo) GetTenantByAPIKeyHash(ctx context.Context, apiKeyHash string) (Tenant, error) {
	var t Tenant
	var isolationTier string
	err := r.pool.QueryRow(ctx,
		`SELECT t.id, t.name, t.isolation_tier, t.region, t.status, t.created_at
		 FROM tenants t
		 JOIN tenant_api_keys k ON k.tenant_id = t.id
		 WHERE k.key_hash = $1 AND k.revoked_at IS NULL`,
		apiKeyHash,
	).Scan(&t.ID, &t.Name, &isolationTier, &t.Region, &t.Status, &t.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return Tenant{}, ErrNotFound
	}
	if err != nil {
		return Tenant{}, fmt.Errorf("tenancy: GetTenantByAPIKeyHash: %w", err)
	}
	return t, nil
}

func (r *PostgresRegistryRepo) GetIsolationConfig(ctx context.Context, tenantID uuid.UUID) (IsolationConfig, error) {
	var c IsolationConfig
	c.TenantID = tenantID
	var schemaName, clusterRef *string
	err := r.pool.QueryRow(ctx,
		`SELECT shard_id, schema_name, cluster_ref FROM tenant_isolation_config WHERE tenant_id = $1`,
		tenantID,
	).Scan(&c.ShardID, &schemaName, &clusterRef)
	if errors.Is(err, pgx.ErrNoRows) {
		return IsolationConfig{}, ErrNotFound
	}
	if err != nil {
		return IsolationConfig{}, fmt.Errorf("tenancy: GetIsolationConfig: %w", err)
	}
	if schemaName != nil {
		c.SchemaName = *schemaName
	}
	if clusterRef != nil {
		c.ClusterRef = *clusterRef
	}
	return c, nil
}

func (r *PostgresRegistryRepo) GetQuota(ctx context.Context, tenantID uuid.UUID) (Quota, error) {
	var q Quota
	q.TenantID = tenantID
	err := r.pool.QueryRow(ctx,
		`SELECT ingestion_rate_limit, matching_job_concurrency, storage_quota_bytes FROM tenant_quota WHERE tenant_id = $1`,
		tenantID,
	).Scan(&q.IngestionRateLimit, &q.MatchingJobConcurrency, &q.StorageQuotaBytes)
	if errors.Is(err, pgx.ErrNoRows) {
		return Quota{}, ErrNotFound
	}
	if err != nil {
		return Quota{}, fmt.Errorf("tenancy: GetQuota: %w", err)
	}
	return q, nil
}

func (r *PostgresRegistryRepo) IsFeatureEnabled(ctx context.Context, tenantID uuid.UUID, flag string) (bool, error) {
	var enabled bool
	err := r.pool.QueryRow(ctx,
		`SELECT enabled FROM tenant_feature_flags WHERE tenant_id = $1 AND flag_key = $2`,
		tenantID, flag,
	).Scan(&enabled)
	if errors.Is(err, pgx.ErrNoRows) {
		// Absence of a row means the flag has never been set for this
		// tenant - treat as disabled rather than an error, since most
		// flags default off and callers shouldn't have to special-case
		// ErrNotFound for every flag check.
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("tenancy: IsFeatureEnabled: %w", err)
	}
	return enabled, nil
}

// CachedRegistryRepo wraps a RegistryRepo with Redis caching for the two
// lookups plans/docs/01-multi-tenancy.md §2.2 explicitly calls out as
// needing it: API-key resolution (hit on every authenticated request) and
// isolation-config resolution. GetTenant/GetQuota/IsFeatureEnabled pass
// through uncached - they're not on the stated hot path and adding cache
// invalidation for tenant_quota/feature-flag updates (which change more
// often, e.g. via admin action) isn't worth the staleness risk without a
// documented need for it.
type CachedRegistryRepo struct {
	RegistryRepo
	redis *redis.Client
	ttl   time.Duration
}

func NewCachedRegistryRepo(inner RegistryRepo, rdb *redis.Client) *CachedRegistryRepo {
	return &CachedRegistryRepo{RegistryRepo: inner, redis: rdb, ttl: 5 * time.Minute}
}

func (c *CachedRegistryRepo) GetTenantByAPIKeyHash(ctx context.Context, apiKeyHash string) (Tenant, error) {
	cacheKey := "tenancy:apikey:" + apiKeyHash

	if cached, err := c.redis.Get(ctx, cacheKey).Result(); err == nil {
		var t Tenant
		if jsonErr := json.Unmarshal([]byte(cached), &t); jsonErr == nil {
			return t, nil
		}
	}

	t, err := c.RegistryRepo.GetTenantByAPIKeyHash(ctx, apiKeyHash)
	if err != nil {
		return Tenant{}, err
	}

	if b, err := json.Marshal(t); err == nil {
		_ = c.redis.Set(ctx, cacheKey, b, c.ttl).Err()
	}
	return t, nil
}

func (c *CachedRegistryRepo) GetIsolationConfig(ctx context.Context, tenantID uuid.UUID) (IsolationConfig, error) {
	cacheKey := "tenancy:isolation:" + tenantID.String()

	if cached, err := c.redis.Get(ctx, cacheKey).Result(); err == nil {
		var cfg IsolationConfig
		if jsonErr := json.Unmarshal([]byte(cached), &cfg); jsonErr == nil {
			return cfg, nil
		}
	}

	cfg, err := c.RegistryRepo.GetIsolationConfig(ctx, tenantID)
	if err != nil {
		return IsolationConfig{}, err
	}

	if b, err := json.Marshal(cfg); err == nil {
		_ = c.redis.Set(ctx, cacheKey, b, c.ttl).Err()
	}
	return cfg, nil
}
