package cases

import (
	"context"

	"github.com/google/uuid"

	"github.com/koriebruh/Jengine/internal/domain"
	"github.com/koriebruh/Jengine/internal/tenancy"
)

// TemporalEnabledFlagKey is the tenant_feature_flags row this task's
// cutover gates on (plans/task/core/20 Implementation Notes: "gate new-
// Break creation and all transition entrypoints on a per-tenant
// cases.temporal_enabled feature flag so migration can roll out
// tenant-by-tenant rather than a single big-bang flag day"). Reuses
// tenancy's existing IsFeatureEnabled mechanism (plans/task/core/04) -
// no new migration needed, that table/lookup already exists.
const TemporalEnabledFlagKey = "cases.temporal_enabled"

// FeatureFlagChecker is the surface FeatureFlagLifecycleService needs -
// tenancy.RegistryRepo satisfies it structurally.
type FeatureFlagChecker interface {
	IsFeatureEnabled(ctx context.Context, tenantID uuid.UUID, flag string) (bool, error)
}

// FeatureFlagLifecycleService dispatches every call to either Postgres
// (legacy, task 13) or Temporal (task 20) is behind, keyed by the
// calling tenant's cases.temporal_enabled flag - the actual
// LifecycleService wired into callers (cmd/coreapi) during the
// tenant-by-tenant migration window. Once every tenant in an
// environment is flipped and backfilled, task 13's old code path can be
// removed as its own follow-up commit (plans/task/core/20's own
// expand-contract instruction - not bundled into the flag-flip itself).
type FeatureFlagLifecycleService struct {
	Flags    FeatureFlagChecker
	Postgres LifecycleService
	Temporal LifecycleService
}

func NewFeatureFlagLifecycleService(flags FeatureFlagChecker, pg, temporal LifecycleService) *FeatureFlagLifecycleService {
	return &FeatureFlagLifecycleService{Flags: flags, Postgres: pg, Temporal: temporal}
}

// resolve picks the backing implementation for tenantID. Errors from
// IsFeatureEnabled fail closed to Postgres (the safer, already-proven
// default) rather than erroring the whole call - a transient flag-
// lookup failure must never block break creation/transitions entirely.
func (s *FeatureFlagLifecycleService) resolve(ctx context.Context, tenantID uuid.UUID) LifecycleService {
	enabled, err := s.Flags.IsFeatureEnabled(ctx, tenantID, TemporalEnabledFlagKey)
	if err != nil || !enabled {
		return s.Postgres
	}
	return s.Temporal
}

// resolveFromCtx is for methods taking only a bare breakID (no
// tenantID param) - tenancy.MustTenantFromContext(ctx) is the only way
// to resolve which tenant, matching LifecycleService's own documented
// contract (see cases.LifecycleService's doc comment).
func (s *FeatureFlagLifecycleService) resolveFromCtx(ctx context.Context) LifecycleService {
	return s.resolve(ctx, tenancy.MustTenantFromContext(ctx).TenantID)
}

func (s *FeatureFlagLifecycleService) OpenBreak(ctx context.Context, params OpenBreakParams) (domain.Case, error) {
	return s.resolve(ctx, params.TenantID).OpenBreak(ctx, params)
}

func (s *FeatureFlagLifecycleService) Assign(ctx context.Context, breakID uuid.UUID, assignee string, actor Actor) error {
	return s.resolveFromCtx(ctx).Assign(ctx, breakID, assignee, actor)
}

func (s *FeatureFlagLifecycleService) Transition(ctx context.Context, breakID uuid.UUID, to BreakStatus, actor Actor, comment string) error {
	return s.resolveFromCtx(ctx).Transition(ctx, breakID, to, actor, comment)
}

func (s *FeatureFlagLifecycleService) AddComment(ctx context.Context, breakID uuid.UUID, actor Actor, body string) (domain.CaseComment, error) {
	return s.resolveFromCtx(ctx).AddComment(ctx, breakID, actor, body)
}

func (s *FeatureFlagLifecycleService) RequestApproval(ctx context.Context, breakID uuid.UUID, actor Actor) error {
	return s.resolveFromCtx(ctx).RequestApproval(ctx, breakID, actor)
}

func (s *FeatureFlagLifecycleService) DecideApproval(ctx context.Context, breakID uuid.UUID, approver Actor, approve bool, comment string) error {
	return s.resolveFromCtx(ctx).DecideApproval(ctx, breakID, approver, approve, comment)
}

func (s *FeatureFlagLifecycleService) TagRootCause(ctx context.Context, breakID uuid.UUID, category string, actor Actor) error {
	return s.resolveFromCtx(ctx).TagRootCause(ctx, breakID, category, actor)
}

func (s *FeatureFlagLifecycleService) BulkAssign(ctx context.Context, breakIDs []uuid.UUID, assignee string, actor Actor) (BulkResult, error) {
	return s.resolveFromCtx(ctx).BulkAssign(ctx, breakIDs, assignee, actor)
}

func (s *FeatureFlagLifecycleService) BulkTransition(ctx context.Context, breakIDs []uuid.UUID, to BreakStatus, actor Actor, comment string) (BulkResult, error) {
	return s.resolveFromCtx(ctx).BulkTransition(ctx, breakIDs, to, actor, comment)
}

func (s *FeatureFlagLifecycleService) BulkAddComment(ctx context.Context, breakIDs []uuid.UUID, actor Actor, body string) (BulkResult, error) {
	return s.resolveFromCtx(ctx).BulkAddComment(ctx, breakIDs, actor, body)
}

var _ LifecycleService = (*FeatureFlagLifecycleService)(nil)
