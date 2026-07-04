//go:build ignore

// This fixture is deliberately non-compliant: GetAccount is missing a
// tenantID/TenantContext parameter. scripts/lint/check_tenant_id.sh must
// flag it. See plans/task/core/01 Implementation Notes.
//
// Excluded from normal builds via the ignore tag above - this file is
// text scanned by the checker script, never compiled as part of the module.
package testdata

import "context"

type AccountRepository struct{}

func (r *AccountRepository) GetAccount(ctx context.Context, accountID string) (any, error) {
	return nil, nil
}
