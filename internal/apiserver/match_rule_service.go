package apiserver

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"connectrpc.com/connect"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/shopspring/decimal"

	jenginev1 "github.com/koriebruh/Jengine/gen/go/jengine/v1"
	"github.com/koriebruh/Jengine/gen/go/jengine/v1/jenginev1connect"
	"github.com/koriebruh/Jengine/internal/domain"
	"github.com/koriebruh/Jengine/internal/matching/core"
	"github.com/koriebruh/Jengine/internal/matching/rules"
	"github.com/koriebruh/Jengine/internal/platform/authz"
	"github.com/koriebruh/Jengine/internal/tenancy"
)

// MatchRuleServiceHandler implements jenginev1connect.MatchRuleServiceHandler -
// intentionally minimal per plans/task/core/15 Implementation Notes: raw
// YAML/JSON authoring (plans/docs/14-dashboard-frontend.md §14.4 - MVP
// has no rule-builder UI), no backtesting sandbox.
type MatchRuleServiceHandler struct {
	Pool        *pgxpool.Pool
	Rules       domain.MatchRuleRepository
	Registry    core.ScoringRegistry
	Idempotency IdempotencyStore
	Authz       *authz.Middleware
}

func matchRuleToProto(r domain.MatchRule) *jenginev1.MatchRule {
	sourceID, targetID := "", ""
	if r.SourceAccountID != nil {
		sourceID = r.SourceAccountID.String()
	}
	if r.TargetAccountID != nil {
		targetID = r.TargetAccountID.String()
	}
	approvedBy := ""
	if r.ApprovedBy != nil {
		approvedBy = *r.ApprovedBy
	}
	var effectiveFrom time.Time
	if r.EffectiveFrom != nil {
		effectiveFrom = *r.EffectiveFrom
	}
	return &jenginev1.MatchRule{
		Id: r.ID.String(), Name: r.Name, Version: int32(r.Version), Status: string(r.Status),
		RuleSpecYaml: string(r.RuleSpec), MatchType: string(r.MatchType),
		SourceAccountId: sourceID, TargetAccountId: targetID, Priority: int32(r.Priority),
		AutoMatchThreshold: r.AutoMatchThreshold.String(), CreatedBy: r.CreatedBy, ApprovedBy: approvedBy,
		EffectiveFrom: toTimestamp(effectiveFrom), CreatedAt: toTimestamp(r.CreatedAt),
	}
}

func (h *MatchRuleServiceHandler) CreateDraftRule(ctx context.Context, req *connect.Request[jenginev1.CreateDraftRuleRequest]) (*connect.Response[jenginev1.CreateDraftRuleResponse], error) {
	tenantID := tenancy.MustTenantFromContext(ctx).TenantID
	idemKey := req.Header().Get("Idempotency-Key")
	hash, err := ComputeRequestHash(tenantID, req.Spec().Procedure, req.Msg)
	if err != nil {
		return nil, err
	}

	sourceAccountID, err := parseUUID("source_account_id", req.Msg.SourceAccountId)
	if err != nil {
		return nil, err
	}
	targetAccountID, err := parseUUID("target_account_id", req.Msg.TargetAccountId)
	if err != nil {
		return nil, err
	}

	spec, err := rules.ParseYAML([]byte(req.Msg.RuleSpecYaml))
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("apiserver: parse rule spec: %w", err))
	}
	// Compile() run purely for early validation (unregistered scoring
	// method, MANY_TO_MANY cardinality, etc.) - the result itself isn't
	// stored; task 12's worker re-parses+compiles the stored rule_spec
	// JSON at partition-load time (see internal/matching/batch/jobs.go).
	if _, err := rules.Compile(spec, h.Registry); err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("apiserver: compile rule spec: %w", err))
	}
	specJSON, err := json.Marshal(spec)
	if err != nil {
		return nil, fmt.Errorf("apiserver: marshal rule spec: %w", err)
	}

	resp, err := WithIdempotency(ctx, h.Idempotency, tenantID, idemKey, hash,
		func() *jenginev1.CreateDraftRuleResponse { return &jenginev1.CreateDraftRuleResponse{} },
		func(ctx context.Context) (*jenginev1.CreateDraftRuleResponse, error) {
			autoMatchThreshold := decimal.NewFromFloat(spec.Rule.Thresholds.AutoMatch)
			var created domain.MatchRule
			err := withTx(ctx, h.Pool, func(ctx context.Context) error {
				var err error
				created, err = h.Rules.Create(ctx, tenantID, domain.MatchRule{
					Name: spec.Rule.Name, Version: max(spec.Rule.Version, 1), Status: domain.MatchRuleStatusDraft,
					RuleSpec: specJSON, MatchType: domain.MatchRuleTypeComposite,
					SourceAccountID: &sourceAccountID, TargetAccountID: &targetAccountID,
					Priority: spec.Rule.Execution.Priority, AutoMatchThreshold: autoMatchThreshold,
					CreatedBy: "api", // real actor identity is a v1/task-23 concern, see auth.go
				})
				return err
			})
			if err != nil {
				return nil, fmt.Errorf("apiserver: create draft rule: %w", err)
			}
			return &jenginev1.CreateDraftRuleResponse{Rule: matchRuleToProto(created)}, nil
		})
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(resp), nil
}

func (h *MatchRuleServiceHandler) ActivateRule(ctx context.Context, req *connect.Request[jenginev1.ActivateRuleRequest]) (*connect.Response[jenginev1.ActivateRuleResponse], error) {
	tenantID := tenancy.MustTenantFromContext(ctx).TenantID
	idemKey := req.Header().Get("Idempotency-Key")
	hash, err := ComputeRequestHash(tenantID, req.Spec().Procedure, req.Msg)
	if err != nil {
		return nil, err
	}

	id, err := parseUUID("id", req.Msg.Id)
	if err != nil {
		return nil, err
	}

	resp, err := WithIdempotency(ctx, h.Idempotency, tenantID, idemKey, hash,
		func() *jenginev1.ActivateRuleResponse { return &jenginev1.ActivateRuleResponse{} },
		func(ctx context.Context) (*jenginev1.ActivateRuleResponse, error) {
			var activated domain.MatchRule
			err := withTx(ctx, h.Pool, func(ctx context.Context) error {
				rule, err := h.Rules.GetByID(ctx, tenantID, id)
				if err != nil {
					return fmt.Errorf("load rule: %w", err)
				}
				// approved_by must be present (basic request validation,
				// not an authorization decision - OPA below decides WHO
				// may approve, this just rejects an obviously-incomplete
				// request before bothering to ask).
				if req.Msg.ApprovedBy == "" {
					return connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("apiserver: approved_by must be set"))
				}
				// Maker-checker via real OPA policy (plans/task/core/23),
				// evaluated against the AUTHENTICATED caller (not the
				// client-supplied approved_by string, which is only the
				// audit-trail value recorded below) - deploy/opa/policies/authz.rego's
				// "rule.activate" rule enforces maker != checker plus role
				// membership; replaces this handler's former inline Go
				// if/else (this task's own named Common Pitfall).
				if err := h.Authz.Authorize(ctx, "rule.activate", authz.ResourceRef{MakerUserID: rule.CreatedBy}); err != nil {
					return err
				}

				// Archive the previously active version for this exact
				// account pair sharing the same rule name, if any.
				active, err := h.Rules.ListActive(ctx, tenantID, *rule.SourceAccountID, *rule.TargetAccountID)
				if err != nil {
					return fmt.Errorf("list active rules: %w", err)
				}
				for _, a := range active {
					if a.Name == rule.Name && a.ID != rule.ID {
						if err := h.Rules.UpdateStatus(ctx, tenantID, a.ID, domain.MatchRuleStatusArchived, nil); err != nil {
							return fmt.Errorf("archive previous active rule %s: %w", a.ID, err)
						}
					}
				}

				approvedBy := req.Msg.ApprovedBy
				if err := h.Rules.UpdateStatus(ctx, tenantID, id, domain.MatchRuleStatusActive, &approvedBy); err != nil {
					return fmt.Errorf("activate rule: %w", err)
				}
				activated, err = h.Rules.GetByID(ctx, tenantID, id)
				return err
			})
			if err != nil {
				return nil, err
			}
			return &jenginev1.ActivateRuleResponse{Rule: matchRuleToProto(activated)}, nil
		})
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(resp), nil
}

func (h *MatchRuleServiceHandler) GetRule(ctx context.Context, req *connect.Request[jenginev1.GetRuleRequest]) (*connect.Response[jenginev1.GetRuleResponse], error) {
	tenantID := tenancy.MustTenantFromContext(ctx).TenantID
	id, err := parseUUID("id", req.Msg.Id)
	if err != nil {
		return nil, err
	}

	var rule domain.MatchRule
	err = withTx(ctx, h.Pool, func(ctx context.Context) error {
		var err error
		rule, err = h.Rules.GetByID(ctx, tenantID, id)
		return err
	})
	if err != nil {
		return nil, fmt.Errorf("apiserver: get rule: %w", err)
	}
	return connect.NewResponse(&jenginev1.GetRuleResponse{Rule: matchRuleToProto(rule)}), nil
}

func (h *MatchRuleServiceHandler) ListRules(ctx context.Context, req *connect.Request[jenginev1.ListRulesRequest]) (*connect.Response[jenginev1.ListRulesResponse], error) {
	tenantID := tenancy.MustTenantFromContext(ctx).TenantID

	var ruleList []domain.MatchRule
	err := withTx(ctx, h.Pool, func(ctx context.Context) error {
		var err error
		ruleList, err = h.Rules.ListByTenant(ctx, tenantID, domain.MatchRuleStatus(req.Msg.Status))
		return err
	})
	if err != nil {
		return nil, fmt.Errorf("apiserver: list rules: %w", err)
	}

	out := make([]*jenginev1.MatchRule, len(ruleList))
	for i, r := range ruleList {
		out[i] = matchRuleToProto(r)
	}
	return connect.NewResponse(&jenginev1.ListRulesResponse{Rules: out}), nil
}

var _ jenginev1connect.MatchRuleServiceHandler = (*MatchRuleServiceHandler)(nil)
