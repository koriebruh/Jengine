// Package workflow implements plans/task/core/20's Temporal-orchestrated
// case lifecycle: BreakLifecycleWorkflow (the parent, one per Break) and
// ApprovalWorkflow (a child workflow for maker-checker gates). All side
// effects (Postgres writes, outbox emission) happen in Activities, never
// directly in workflow function bodies - Temporal workflows must stay
// deterministic/replayable (this task's own Common Pitfalls).
//
// This package intentionally does NOT import internal/cases - it
// defines its own local Actor type mirroring cases.Actor's shape,
// matching this codebase's established "mirror without importing"
// convention (see e.g. cases.OpenBreakParams' own doc comment) - the
// adapter living in internal/cases (temporal_lifecycle.go) is what
// bridges the two, converting at the boundary, since internal/cases
// already defines cases.LifecycleService (the interface this task's
// Temporal-backed implementation must satisfy) and importing this
// package FROM there would be a one-way dependency, not a cycle, only
// if this package doesn't import back.
package workflow

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"

	"github.com/koriebruh/Jengine/internal/audit"
	"github.com/koriebruh/Jengine/internal/domain"
)

// Actor mirrors cases.Actor's shape.
type Actor struct {
	UserID string
	Role   string
}

// TxRunner wraps fn in a transaction scoped to tenantID - same shape as
// every other package's own local copy in this codebase.
type TxRunner func(ctx context.Context, tenantID uuid.UUID, fn func(ctx context.Context) error) error

// InsertOutbox writes one outbox_event row - satisfied by
// internal/platform/outbox.Insert via a small adapter closure the
// caller supplies (see Activities.InsertOutbox's own doc comment for
// why this is a func field rather than importing outbox/pgx directly).
type InsertOutbox func(ctx context.Context, tenantID uuid.UUID, aggregateID uuid.UUID, eventType, topic string, payload []byte) error

// Activities holds every dependency plans/task/core/20's five Activities
// need. A single struct (Temporal's own convention: register bound
// methods via worker.RegisterActivity(activities.SomeMethod)) rather
// than five separate types, since they share the same dependencies.
type Activities struct {
	TxRunner TxRunner
	Cases    domain.CaseRepository
	Audit    audit.Writer
	Routing  domain.CaseRoutingConfigRepository
	// InsertOutbox is a func field (not internal/platform/outbox.Insert
	// called directly) so this package doesn't need to import
	// internal/storage/postgres for postgres.TxFromContext just to get
	// a pgx.Tx - the caller (internal/cases's adapter) already has that
	// wiring, matching internal/matching/reconcile's own use of
	// outbox.Insert as the precedent for this pattern.
	InsertOutbox InsertOutbox
}

// --- AutoAssignActivity (plans/docs/05-case-management.md §6.2) ---

type AutoAssignInput struct {
	TenantID     uuid.UUID
	BreakID      uuid.UUID
	BreakType    string
	AccountID    uuid.UUID
	AmountAtRisk decimal.Decimal
}

type AutoAssignResult struct {
	Assignee string
}

// routingConfig is case_routing_configs.config's shape - see that
// migration's own comment for the strategy field's documented (but not
// all yet implemented) values.
type routingConfig struct {
	Strategy    string   `json:"strategy"`
	TeamMembers []string `json:"team_members"`
}

// defaultTeamMembers is the fallback when a tenant has no ACTIVE
// case_routing_configs row yet - a real, working round-robin default
// rather than an error, since "no routing config configured yet" is an
// expected, valid tenant state (plans/task/core/20 Implementation
// Notes only requires ONE real strategy at MVP - round-robin - with the
// schema/config shape open for load-balanced/skill-based/root-cause-
// mapping strategies to be added later without a schema change).
var defaultTeamMembers = []string{"unassigned-pool"}

// AutoAssignActivity picks an assignee via round-robin over the
// tenant's configured team_members list (or defaultTeamMembers if none
// configured) - net-new logic, plans/task/core/13's MVP only supported
// manual assignment. round-robin state (an index) is derived
// deterministically from a hash of BreakID rather than external
// counter state, so repeated Activity retries (Temporal's own retry
// policy) are idempotent - the same BreakID always resolves to the same
// assignee even after a retry, rather than silently advancing the
// round-robin position twice for one logical assignment.
func (a *Activities) AutoAssignActivity(ctx context.Context, in AutoAssignInput) (AutoAssignResult, error) {
	members := defaultTeamMembers
	err := a.TxRunner(ctx, in.TenantID, func(ctx context.Context) error {
		cfg, err := a.Routing.GetActive(ctx, in.TenantID)
		if err != nil {
			return nil //nolint:nilerr // no ACTIVE config is expected/valid - fall through to defaultTeamMembers, not an error
		}
		var rc routingConfig
		if jsonErr := jsonUnmarshal(cfg.Config, &rc); jsonErr != nil {
			return fmt.Errorf("workflow: unmarshal routing config: %w", jsonErr)
		}
		if len(rc.TeamMembers) > 0 {
			members = rc.TeamMembers
		}
		return nil
	})
	if err != nil {
		return AutoAssignResult{}, err
	}

	idx := hashToIndex(in.BreakID, len(members))
	return AutoAssignResult{Assignee: members[idx]}, nil
}

// --- ComputeSLAActivity (plans/docs/05-case-management.md §6.3) ---

type ComputeSLAInput struct {
	TenantID uuid.UUID
	OpenedAt time.Time
	Priority string
}

type ComputeSLAResult struct {
	SLADueAt time.Time
}

// defaultSLAByPriority is a simple, real, working SLA policy - business-
// hour-aware calendars (the design doc's stretch goal, shared with
// matching's own date-window logic) are explicitly out of scope for
// this task (Non-Goals don't list it, but plans/docs/11-scalability-roadmap.md
// §12.2 Phase 0 doesn't call for a calendar service either); plain
// wall-clock durations are the honest MVP answer.
var defaultSLAByPriority = map[string]time.Duration{
	"HIGH":   4 * time.Hour,
	"MEDIUM": 24 * time.Hour,
	"LOW":    72 * time.Hour,
}

func (a *Activities) ComputeSLAActivity(_ context.Context, in ComputeSLAInput) (ComputeSLAResult, error) {
	d, ok := defaultSLAByPriority[in.Priority]
	if !ok {
		d = defaultSLAByPriority["MEDIUM"]
	}
	return ComputeSLAResult{SLADueAt: in.OpenedAt.Add(d)}, nil
}

// --- AuthorizeApprovalActivity (plans/docs/05-case-management.md §6.4) ---

type AuthorizeApprovalInput struct {
	TenantID       uuid.UUID
	MakerUserID    string
	ApproverUserID string
	ApproverRole   string
}

type AuthorizeApprovalResult struct {
	Authorized bool
	Reason     string
}

// AuthorizeApprovalActivity is a SIMPLE ROLE-CHECK STUB by design
// (plans/task/core/20 Implementation Notes: "core task 23 later swaps
// the activity's internals for a real OPA/Rego-backed decision. Keep
// the Activity's function signature stable across that swap"). Checks
// only maker != checker and a coarse role allowlist - no per-account/
// business-unit ABAC (that's task 23).
func (a *Activities) AuthorizeApprovalActivity(_ context.Context, in AuthorizeApprovalInput) (AuthorizeApprovalResult, error) {
	if in.ApproverUserID == in.MakerUserID {
		return AuthorizeApprovalResult{Authorized: false, Reason: "maker != checker violation: approver is the same user who made the request"}, nil
	}
	switch in.ApproverRole {
	case "Approver", "Recon Manager", "Tenant Admin":
		return AuthorizeApprovalResult{Authorized: true}, nil
	default:
		return AuthorizeApprovalResult{Authorized: false, Reason: fmt.Sprintf("role %q is not authorized to approve", in.ApproverRole)}, nil
	}
}

// --- PersistTransitionActivity ---

type PersistTransitionInput struct {
	TenantID  uuid.UUID
	BreakID   uuid.UUID
	From, To  domain.CaseStatus
	Actor     Actor
	Comment   string
	Assignee  string // set only for assign transitions; empty otherwise
	EventType string // e.g. "break.assigned", "break.transitioned" - mirrors cases.PostgresLifecycleService's own event-type-per-action convention
}

// PersistTransitionActivity is the one place BreakLifecycleWorkflow's
// state changes actually reach Postgres - writes the new status (and
// assignee, if set), an optional comment, and both audit trails
// (CaseAuditEvent + the global hash-chained AuditEvent, same two-tier
// model plans/task/core/13's PostgresLifecycleService already
// established - see that type's recordAuditEvent for the precedent this
// mirrors).
func (a *Activities) PersistTransitionActivity(ctx context.Context, in PersistTransitionInput) error {
	return a.TxRunner(ctx, in.TenantID, func(ctx context.Context) error {
		if err := a.Cases.UpdateStatus(ctx, in.TenantID, in.BreakID, in.To); err != nil {
			return fmt.Errorf("workflow: persist transition: update status: %w", err)
		}
		if in.Assignee != "" {
			if err := a.Cases.UpdateAssignee(ctx, in.TenantID, in.BreakID, in.Assignee); err != nil {
				return fmt.Errorf("workflow: persist transition: update assignee: %w", err)
			}
		}
		if in.Comment != "" {
			if _, err := a.Cases.AddComment(ctx, in.TenantID, domain.CaseComment{
				CaseID: in.BreakID, Actor: in.Actor.UserID, EventType: "comment",
				Payload: mustJSON(map[string]string{"body": in.Comment}),
			}); err != nil {
				return fmt.Errorf("workflow: persist transition: add comment: %w", err)
			}
		}

		afterJSON := mustJSON(map[string]string{"from": string(in.From), "to": string(in.To)})
		if _, err := a.Cases.AddAuditEvent(ctx, in.TenantID, domain.CaseAuditEvent{
			CaseID: in.BreakID, Actor: in.Actor.UserID, EventType: in.EventType, Payload: afterJSON,
		}); err != nil {
			return fmt.Errorf("workflow: persist transition: add case audit event: %w", err)
		}
		if err := a.Audit.Write(ctx, audit.AuditEvent{
			TenantID: in.TenantID, ActorID: in.Actor.UserID, ActorType: "USER",
			EventType: in.EventType, EntityType: "Break", EntityID: in.BreakID.String(),
			AfterState: afterJSON,
		}); err != nil {
			return fmt.Errorf("workflow: persist transition: write global audit event: %w", err)
		}
		return nil
	})
}

// --- EmitOutboxEventActivity ---

type EmitOutboxEventInput struct {
	TenantID    uuid.UUID
	AggregateID uuid.UUID
	EventType   string
	Topic       string
	Payload     []byte
}

// EmitOutboxEventActivity writes an outbox_event row (plans/task/core/18's
// pattern) - e.g. break.sla_breached, case.approval_requested,
// consumed by task 21's outbound webhook dispatcher. A separate
// Activity from PersistTransitionActivity (not folded into one call) -
// if this one fails after the transition already persisted, Temporal's
// own Activity retry policy re-runs just this Activity until it
// succeeds, which is the correct behavior for Temporal's execution
// model (no need to force single-database-transaction atomicity across
// two independently-retried Activities the way plans/task/core/18's
// outbox.Insert does within one Activity/request).
func (a *Activities) EmitOutboxEventActivity(ctx context.Context, in EmitOutboxEventInput) error {
	return a.InsertOutbox(ctx, in.TenantID, in.AggregateID, in.EventType, in.Topic, in.Payload)
}
