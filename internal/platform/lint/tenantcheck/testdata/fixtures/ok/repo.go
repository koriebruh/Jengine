// Package ok is a compliant fixture for tenantcheck_test.go - every
// exported method takes context.Context first and either an explicit
// tenantID uuid.UUID parameter or calls tenancy.MustTenantFromContext.
package ok

import (
	"context"

	"github.com/google/uuid"

	"github.com/koriebruh/Jengine/internal/tenancy"
)

type Repo struct{}

func (r *Repo) GoodWithTenantID(ctx context.Context, tenantID uuid.UUID, id string) error {
	return nil
}

func (r *Repo) GoodWithMustTenant(ctx context.Context, id string) error {
	_ = tenancy.MustTenantFromContext(ctx)
	return nil
}
