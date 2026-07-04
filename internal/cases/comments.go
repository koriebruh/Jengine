package cases

import (
	"context"

	"github.com/google/uuid"

	"github.com/koriebruh/Jengine/internal/domain"
	"github.com/koriebruh/Jengine/internal/tenancy"
)

// ListComments is a read convenience wrapper over
// domain.CaseRepository.ListComments (task 05 already built the
// append-only writer/reader pair at the repository layer - AddComment in
// lifecycle.go calls the same Create path). Not part of the
// LifecycleService interface (reads don't need the swap-boundary
// treatment task 20's Temporal migration cares about), but exposed here
// as a concrete-type method so callers (task 15's API layer, tests)
// don't need to reach into internal/storage/postgres directly.
func (s *PostgresLifecycleService) ListComments(ctx context.Context, breakID uuid.UUID) ([]domain.CaseComment, error) {
	tenantID := tenancy.MustTenantFromContext(ctx).TenantID
	return s.Cases.ListComments(ctx, tenantID, breakID)
}
