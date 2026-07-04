package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/koriebruh/Jengine/internal/ingestion/dedup"
)

const reuploadPolicySettingKey = "file_reupload_policy"

// TenantSettingsRepo reads the generic tenant_settings key/value store
// (plans/task/core/03/04's Tenant Registry - unsharded, no RLS, since
// it's always looked up by an explicit tenant_id filter rather than an
// ambient session variable; the method below still takes an explicit
// tenantID parameter and filters by it directly, so it passes the
// tenancy lint check on its own merits, without needing an exemption).
type TenantSettingsRepo struct {
	pool *pgxpool.Pool
}

func NewTenantSettingsRepo(pool *pgxpool.Pool) *TenantSettingsRepo {
	return &TenantSettingsRepo{pool: pool}
}

func (r *TenantSettingsRepo) GetReuploadPolicy(ctx context.Context, tenantID uuid.UUID) (dedup.ReuploadPolicy, error) {
	var raw json.RawMessage
	err := r.pool.QueryRow(ctx,
		`SELECT value FROM tenant_settings WHERE tenant_id = $1 AND key = $2`,
		tenantID, reuploadPolicySettingKey,
	).Scan(&raw)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", nil // unset - caller applies the safe default
	}
	if err != nil {
		return "", fmt.Errorf("postgres: TenantSettingsRepo.GetReuploadPolicy: %w", err)
	}

	var policy dedup.ReuploadPolicy
	if err := json.Unmarshal(raw, &policy); err != nil {
		return "", fmt.Errorf("postgres: TenantSettingsRepo.GetReuploadPolicy: unmarshal: %w", err)
	}
	return policy, nil
}

var _ dedup.TenantReuploadPolicyLookup = (*TenantSettingsRepo)(nil)
