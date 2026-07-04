//go:build ignore

// This fixture is compliant: GetAccount takes an explicit tenantID
// parameter. scripts/lint/check_tenant_id.sh must NOT flag it.
//
// Excluded from normal builds via the ignore tag above - this file is
// text scanned by the checker script, never compiled as part of the module.
package testdata

import "context"

type AccountRepository struct{}

func (r *AccountRepository) GetAccount(ctx context.Context, tenantID string, accountID string) (any, error) {
	return nil, nil
}
