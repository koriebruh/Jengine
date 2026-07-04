package cases

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"

	"github.com/koriebruh/Jengine/internal/audit"
	"github.com/koriebruh/Jengine/internal/domain"
	"github.com/koriebruh/Jengine/internal/tenancy"
)

// OpenBreakParams mirrors core.OpenBreakParams' shape without importing
// internal/matching/core here - only breaksink.go (the adapter) imports
// core, keeping this interface free of matching-package types per
// plans/task/core/13's own Common Pitfalls.
type OpenBreakParams struct {
	TenantID       uuid.UUID
	AccountID      uuid.UUID
	TransactionIDs []uuid.UUID
	BreakType      string
	AmountAtRisk   decimal.Decimal
	Currency       string
}

// Actor identifies who is performing a lifecycle action, captured for
// audit purposes - no RBAC/ABAC enforcement happens here (that's task
// 15/task 23, coarse-grained or not at all at MVP).
type Actor struct {
	UserID string
	Role   string
}

// BulkResult reports a per-ID outcome for a bulk operation - a bulk call
// over a heterogeneous selection (mixed current statuses) can legitimately
// partially fail, e.g. bulk-resolving 10 selected breaks where 2 are
// already RESOLVED must not silently succeed-as-a-whole or fail-as-a-
// whole (plans/task/core/13 Implementation Notes).
type BulkResult struct {
	BatchOpID uuid.UUID
	Succeeded []uuid.UUID
	Failed    map[uuid.UUID]string
}

// LifecycleService is the clean swap boundary plans/task/core/20 (V1,
// Temporal migration) will implement against later - free of any
// Postgres-specific types (no transactions, no repository types) so that
// swap doesn't require touching any caller (task 12's BreakSink adapter,
// task 15's API layer).
//
// OpenBreak is the one method that works with a plain, tenant-agnostic
// ctx - its OpenBreakParams already carries TenantID, so it needs no
// pre-set tenant context (this is what makes it safe for task 12's
// BreakSinkAdapter to call from a bare River-job ctx, outside any HTTP
// request's tenancy.Middleware). Every OTHER method here only takes a
// bare breakID with no tenant parameter, so the caller MUST have already
// called tenancy.WithTenant on ctx (e.g. via tenancy.Middleware for a
// real API request, or explicitly in a test) - there is no other way to
// resolve which tenant a bare ID belongs to.
type LifecycleService interface {
	OpenBreak(ctx context.Context, params OpenBreakParams) (domain.Case, error)
	Assign(ctx context.Context, breakID uuid.UUID, assignee string, actor Actor) error
	Transition(ctx context.Context, breakID uuid.UUID, to BreakStatus, actor Actor, comment string) error
	AddComment(ctx context.Context, breakID uuid.UUID, actor Actor, body string) (domain.CaseComment, error)
	RequestApproval(ctx context.Context, breakID uuid.UUID, actor Actor) error
	DecideApproval(ctx context.Context, breakID uuid.UUID, approver Actor, approve bool, comment string) error
	TagRootCause(ctx context.Context, breakID uuid.UUID, category string, actor Actor) error

	BulkAssign(ctx context.Context, breakIDs []uuid.UUID, assignee string, actor Actor) (BulkResult, error)
	BulkTransition(ctx context.Context, breakIDs []uuid.UUID, to BreakStatus, actor Actor, comment string) (BulkResult, error)
	BulkAddComment(ctx context.Context, breakIDs []uuid.UUID, actor Actor, body string) (BulkResult, error)
}

// TxRunner wraps fn in a tenant-scoped transaction - same shape used
// throughout this codebase (mapping.TxRunner, batch.TxRunner, etc.).
type TxRunner func(ctx context.Context, tenantID uuid.UUID, fn func(ctx context.Context) error) error

const approvalRequestedEventType = "approval.requested"

// PostgresLifecycleService is the MVP implementation: plain application
// logic backed by Postgres, no Temporal (plans/docs/11-scalability-roadmap.md
// §12.2 Phase 0 - "a simple state machine + Postgres, no Temporal yet").
type PostgresLifecycleService struct {
	TxRunner TxRunner
	Cases    domain.CaseRepository
	Audit    audit.Writer
}

func NewPostgresLifecycleService(txRunner TxRunner, cases domain.CaseRepository, auditWriter audit.Writer) *PostgresLifecycleService {
	return &PostgresLifecycleService{TxRunner: txRunner, Cases: cases, Audit: auditWriter}
}

func (s *PostgresLifecycleService) OpenBreak(ctx context.Context, params OpenBreakParams) (domain.Case, error) {
	var result domain.Case
	err := s.TxRunner(ctx, params.TenantID, func(ctx context.Context) error {
		c := domain.Case{
			TenantID:              params.TenantID,
			AccountID:             params.AccountID,
			RelatedTransactionIDs: params.TransactionIDs,
			BreakType:             domain.BreakType(params.BreakType),
			Status:                BreakOpen,
			Priority:              "MEDIUM", // MVP default - no priority-assignment policy engine yet
			AmountAtRisk:          &params.AmountAtRisk,
			Currency:              &params.Currency,
		}
		created, err := s.Cases.Create(ctx, params.TenantID, c)
		if err != nil {
			return fmt.Errorf("cases: open break: %w", err)
		}
		result = created

		return s.recordAuditEvent(ctx, created.TenantID, created.ID, Actor{UserID: "system", Role: "SYSTEM"},
			"break.opened", nil, created)
	})
	return result, err
}

func (s *PostgresLifecycleService) Assign(ctx context.Context, breakID uuid.UUID, assignee string, actor Actor) error {
	return s.TxRunner(ctx, tenancy.MustTenantFromContext(ctx).TenantID, func(ctx context.Context) error {
		return s.assignLocked(ctx, breakID, assignee, actor)
	})
}

// assignLocked performs the actual assign + transition-to-ASSIGNED +
// audit write, assuming it's already running inside a transaction (via
// TxRunner) - factored out so bulk operations can call it per-ID inside
// their own single enclosing transaction/audit event.
func (s *PostgresLifecycleService) assignLocked(ctx context.Context, breakID uuid.UUID, assignee string, actor Actor) error {
	tenantID, before, err := s.loadCase(ctx, breakID)
	if err != nil {
		return err
	}

	if !IsValidTransition(before.Status, BreakAssigned) {
		return fmt.Errorf("cases: assign: invalid transition %s -> %s", before.Status, BreakAssigned)
	}

	if err := s.Cases.UpdateStatus(ctx, tenantID, breakID, BreakAssigned); err != nil {
		return fmt.Errorf("cases: assign: update status: %w", err)
	}
	after := before
	after.Status = BreakAssigned
	after.AssignedTo = &assignee

	return s.recordAuditEvent(ctx, tenantID, breakID, actor, "break.assigned", before, after)
}

func (s *PostgresLifecycleService) Transition(ctx context.Context, breakID uuid.UUID, to BreakStatus, actor Actor, comment string) error {
	return s.TxRunner(ctx, tenancy.MustTenantFromContext(ctx).TenantID, func(ctx context.Context) error {
		return s.transitionLocked(ctx, breakID, to, actor, comment)
	})
}

func (s *PostgresLifecycleService) transitionLocked(ctx context.Context, breakID uuid.UUID, to BreakStatus, actor Actor, comment string) error {
	tenantID, before, err := s.loadCase(ctx, breakID)
	if err != nil {
		return err
	}

	if !IsValidTransition(before.Status, to) {
		return fmt.Errorf("cases: transition: invalid transition %s -> %s", before.Status, to)
	}

	if requiresApproval(before.Status, to) {
		requester, err := s.approvalRequester(ctx, breakID)
		if err != nil {
			return err
		}
		if requester != "" && requester == actor.UserID {
			return fmt.Errorf("cases: transition: maker != checker violation - actor %q cannot approve their own request", actor.UserID)
		}
	}

	if err := s.Cases.UpdateStatus(ctx, tenantID, breakID, to); err != nil {
		return fmt.Errorf("cases: transition: update status: %w", err)
	}
	after := before
	after.Status = to

	if comment != "" {
		if _, err := s.Cases.AddComment(ctx, tenantID, domain.CaseComment{
			CaseID: breakID, Actor: actor.UserID, EventType: "comment",
			Payload: mustJSON(map[string]string{"body": comment}),
		}); err != nil {
			return fmt.Errorf("cases: transition: add comment: %w", err)
		}
	}

	return s.recordAuditEvent(ctx, tenantID, breakID, actor, "break.transitioned", before, after)
}

func (s *PostgresLifecycleService) AddComment(ctx context.Context, breakID uuid.UUID, actor Actor, body string) (domain.CaseComment, error) {
	var result domain.CaseComment
	err := s.TxRunner(ctx, tenancy.MustTenantFromContext(ctx).TenantID, func(ctx context.Context) error {
		tenantID, _, err := s.loadCase(ctx, breakID)
		if err != nil {
			return err
		}
		result, err = s.Cases.AddComment(ctx, tenantID, domain.CaseComment{
			CaseID: breakID, Actor: actor.UserID, EventType: "comment",
			Payload: mustJSON(map[string]string{"body": body}),
		})
		if err != nil {
			return fmt.Errorf("cases: add comment: %w", err)
		}
		return s.recordAuditEvent(ctx, tenantID, breakID, actor, "break.commented", nil, map[string]string{"body": body})
	})
	return result, err
}

func (s *PostgresLifecycleService) RequestApproval(ctx context.Context, breakID uuid.UUID, actor Actor) error {
	return s.TxRunner(ctx, tenancy.MustTenantFromContext(ctx).TenantID, func(ctx context.Context) error {
		tenantID, before, err := s.loadCase(ctx, breakID)
		if err != nil {
			return err
		}
		if !IsValidTransition(before.Status, BreakPendingApproval) {
			return fmt.Errorf("cases: request approval: invalid transition %s -> %s", before.Status, BreakPendingApproval)
		}
		if err := s.Cases.UpdateStatus(ctx, tenantID, breakID, BreakPendingApproval); err != nil {
			return fmt.Errorf("cases: request approval: update status: %w", err)
		}

		// Record the requester so DecideApproval/Transition can enforce
		// maker != checker later - no dedicated schema column for this,
		// so it's recorded as a normal audit event and looked up by type
		// (see approvalRequester).
		if err := s.recordAuditEvent(ctx, tenantID, breakID, actor, approvalRequestedEventType, nil, nil); err != nil {
			return err
		}
		return nil
	})
}

func (s *PostgresLifecycleService) DecideApproval(ctx context.Context, breakID uuid.UUID, approver Actor, approve bool, comment string) error {
	to := BreakResolved
	if !approve {
		to = BreakAssigned // rejection returns to work
	}
	return s.TxRunner(ctx, tenancy.MustTenantFromContext(ctx).TenantID, func(ctx context.Context) error {
		return s.transitionLocked(ctx, breakID, to, approver, comment)
	})
}

func (s *PostgresLifecycleService) TagRootCause(ctx context.Context, breakID uuid.UUID, category string, actor Actor) error {
	return s.TxRunner(ctx, tenancy.MustTenantFromContext(ctx).TenantID, func(ctx context.Context) error {
		tenantID, before, err := s.loadCase(ctx, breakID)
		if err != nil {
			return err
		}
		if !IsValidRootCause(category) {
			return fmt.Errorf("cases: tag root cause: unrecognized category %q", category)
		}
		if err := s.Cases.UpdateRootCause(ctx, tenantID, breakID, category); err != nil {
			return err
		}
		after := before
		after.RootCauseCategory = &category
		return s.recordAuditEvent(ctx, tenantID, breakID, actor, "break.root_cause_tagged", before, after)
	})
}

func (s *PostgresLifecycleService) BulkAssign(ctx context.Context, breakIDs []uuid.UUID, assignee string, actor Actor) (BulkResult, error) {
	return s.bulkOp(ctx, breakIDs, actor, "break.bulk_assigned", func(ctx context.Context, id uuid.UUID) error {
		return s.assignLocked(ctx, id, assignee, actor)
	})
}

func (s *PostgresLifecycleService) BulkTransition(ctx context.Context, breakIDs []uuid.UUID, to BreakStatus, actor Actor, comment string) (BulkResult, error) {
	return s.bulkOp(ctx, breakIDs, actor, "break.bulk_transitioned", func(ctx context.Context, id uuid.UUID) error {
		return s.transitionLocked(ctx, id, to, actor, comment)
	})
}

func (s *PostgresLifecycleService) BulkAddComment(ctx context.Context, breakIDs []uuid.UUID, actor Actor, body string) (BulkResult, error) {
	return s.bulkOp(ctx, breakIDs, actor, "break.bulk_commented", func(ctx context.Context, id uuid.UUID) error {
		tenantID, _, err := s.loadCase(ctx, id)
		if err != nil {
			return err
		}
		_, err = s.Cases.AddComment(ctx, tenantID, domain.CaseComment{
			CaseID: id, Actor: actor.UserID, EventType: "comment",
			Payload: mustJSON(map[string]string{"body": body}),
		})
		return err
	})
}

// bulkOp runs perID for every breakID inside ONE transaction (so
// per-item DB writes for succeeded IDs are not rolled back by a
// different ID's failure - each perID call is independently committed
// logic, just sharing one connection/transaction scope for efficiency),
// collects per-ID success/failure, and writes exactly ONE audit event
// for the whole batch (plans/docs/05-case-management.md §6.2: "single
// audit event referencing batch-op id + affected case ids" - not one
// event per break).
func (s *PostgresLifecycleService) bulkOp(ctx context.Context, breakIDs []uuid.UUID, actor Actor, eventType string, perID func(ctx context.Context, id uuid.UUID) error) (BulkResult, error) {
	result := BulkResult{BatchOpID: uuid.New(), Failed: make(map[uuid.UUID]string)}
	if len(breakIDs) == 0 {
		return result, nil
	}

	tenantID := tenancy.MustTenantFromContext(ctx).TenantID
	err := s.TxRunner(ctx, tenantID, func(ctx context.Context) error {
		for _, id := range breakIDs {
			if err := perID(ctx, id); err != nil {
				result.Failed[id] = err.Error()
				continue
			}
			result.Succeeded = append(result.Succeeded, id)
		}

		if len(result.Succeeded) == 0 {
			return nil // nothing to audit if every item failed
		}
		payload := mustJSON(map[string]any{
			"batch_op_id":  result.BatchOpID,
			"break_ids":    result.Succeeded,
			"failed_count": len(result.Failed),
		})
		// One audit event per batch, referencing the FIRST succeeded
		// break as its CaseAuditEvent.CaseID (case_audit_events' schema
		// requires a single case_id per row - the batch_op_id in the
		// payload is what ties the whole batch together for lookup, not
		// this per-row FK) - see plans/task/core/13 Implementation Notes.
		if _, err := s.Cases.AddAuditEvent(ctx, tenantID, domain.CaseAuditEvent{
			CaseID: result.Succeeded[0], Actor: actor.UserID, EventType: eventType, Payload: payload,
		}); err != nil {
			return fmt.Errorf("cases: bulk op: add audit event: %w", err)
		}
		return s.Audit.Write(ctx, audit.AuditEvent{
			TenantID: tenantID, ActorID: actor.UserID, ActorType: "USER",
			EventType: eventType, EntityType: "Break", EntityID: result.BatchOpID.String(),
			AfterState: payload,
		})
	})
	return result, err
}

func (s *PostgresLifecycleService) loadCase(ctx context.Context, breakID uuid.UUID) (uuid.UUID, domain.Case, error) {
	tenantID := tenancy.MustTenantFromContext(ctx).TenantID
	c, err := s.Cases.GetByID(ctx, tenantID, breakID)
	if err != nil {
		return uuid.Nil, domain.Case{}, fmt.Errorf("cases: load break %s: %w", breakID, err)
	}
	return tenantID, c, nil
}

func (s *PostgresLifecycleService) approvalRequester(ctx context.Context, breakID uuid.UUID) (string, error) {
	tenantID := tenancy.MustTenantFromContext(ctx).TenantID
	events, err := s.Cases.ListAuditEvents(ctx, tenantID, breakID)
	if err != nil {
		return "", fmt.Errorf("cases: approval requester: %w", err)
	}
	for i := len(events) - 1; i >= 0; i-- {
		if events[i].EventType == approvalRequestedEventType {
			return events[i].Actor, nil
		}
	}
	return "", nil
}

func (s *PostgresLifecycleService) recordAuditEvent(ctx context.Context, tenantID, breakID uuid.UUID, actor Actor, eventType string, before, after any) error {
	beforeJSON := mustJSONOrNil(before)
	afterJSON := mustJSONOrNil(after)

	// §6.5's two-tier model: the case-level trail (UX-optimized) AND the
	// global hash-chained log (compliance-grade) - both written for
	// every transition/comment, never just one (plans/task/core/13
	// Common Pitfalls: "only writing to CaseAuditEvent and forgetting
	// the global AuditEvent call silently breaks the audit chain's
	// completeness").
	if _, err := s.Cases.AddAuditEvent(ctx, tenantID, domain.CaseAuditEvent{
		CaseID: breakID, Actor: actor.UserID, EventType: eventType, Payload: afterJSON,
	}); err != nil {
		return fmt.Errorf("cases: record case audit event: %w", err)
	}

	if err := s.Audit.Write(ctx, audit.AuditEvent{
		TenantID: tenantID, ActorID: actor.UserID, ActorType: "USER",
		EventType: eventType, EntityType: "Break", EntityID: breakID.String(),
		BeforeState: beforeJSON, AfterState: afterJSON,
	}); err != nil {
		return fmt.Errorf("cases: record global audit event: %w", err)
	}
	return nil
}

func mustJSON(v any) json.RawMessage {
	b, err := json.Marshal(v)
	if err != nil {
		panic(fmt.Sprintf("cases: marshal: %v", err))
	}
	return b
}

func mustJSONOrNil(v any) json.RawMessage {
	if v == nil {
		return nil
	}
	return mustJSON(v)
}

var _ LifecycleService = (*PostgresLifecycleService)(nil)
