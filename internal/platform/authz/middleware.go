package authz

import (
	"context"
	"fmt"

	"connectrpc.com/connect"

	"github.com/koriebruh/Jengine/internal/tenancy"
)

// Middleware wraps an OPAClient with the two integration patterns this
// task's endpoints actually need:
//
//   - Authorize: called explicitly inside a handler once it has loaded
//     the domain data a policy needs (e.g. a MatchRule's CreatedBy, to
//     check maker != checker) - most authz.ResourceRef fields can't be
//     known generically from a request message alone.
//   - UnaryInterceptor: a Connect-RPC interceptor for procedures whose
//     action needs no resource-specific data (coarse role gating) -
//     registered per-procedure via actionFor, not blanket-applied,
//     since most procedures in this codebase have no single fixed
//     action/resource shape.
//
// Either path replaces a hardcoded Go role check with a real OPA
// decision - this task's own Common Pitfall names inline if/else
// authorization logic as the mistake to avoid.
type Middleware struct {
	Client OPAClient
}

func NewMiddleware(client OPAClient) *Middleware {
	return &Middleware{Client: client}
}

// SubjectFromContext builds a Subject from the TenantContext
// tenancy.Middleware already populated (UserID/Roles/BusinessUnit -
// plans/task/core/23's extension to Claims/TenantContext).
func SubjectFromContext(ctx context.Context) Subject {
	tc := tenancy.MustTenantFromContext(ctx)
	roles := make([]Role, len(tc.Roles))
	for i, r := range tc.Roles {
		roles[i] = Role(r)
	}
	return Subject{UserID: tc.UserID, TenantID: tc.TenantID.String(), Roles: roles, BusinessUnit: tc.BusinessUnit}
}

// Authorize evaluates action/resource for the caller in ctx, returning
// nil on allow or a connect.CodePermissionDenied error carrying
// Decision.Reason on deny - the transparency requirement (a denial
// says why, not a bare 403/PermissionDenied).
func (m *Middleware) Authorize(ctx context.Context, action string, resource ResourceRef) error {
	subject := SubjectFromContext(ctx)
	resource.TenantID = subject.TenantID

	decision, err := m.Client.Evaluate(ctx, OPAInput{Subject: subject, Action: action, Resource: resource})
	if err != nil {
		return fmt.Errorf("authz: evaluate policy: %w", err)
	}
	if !decision.Allow {
		return connect.NewError(connect.CodePermissionDenied, fmt.Errorf("authz: %s", decision.Reason))
	}
	return nil
}

// ActionForProcedure maps a Connect-RPC procedure (connect.Spec.Procedure,
// e.g. "/jengine.v1.MatchRuleService/ActivateRule") to the OPA action
// string to evaluate - registered explicitly per procedure rather than
// derived automatically, since procedure naming has no fixed
// correspondence to policy action names.
type ActionForProcedure func(procedure string) (action string, ok bool)

// UnaryInterceptor enforces actionFor's mapped action for every
// registered procedure, with a resource-less ResourceRef (only the
// subject's role/business-unit matter, no per-call domain lookup) -
// procedures actionFor doesn't recognize pass through unauthorized by
// this interceptor (they may still be gated by an explicit Authorize
// call inside their own handler, or need no authz at all).
func (m *Middleware) UnaryInterceptor(actionFor ActionForProcedure) connect.UnaryInterceptorFunc {
	return func(next connect.UnaryFunc) connect.UnaryFunc {
		return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
			action, ok := actionFor(req.Spec().Procedure)
			if !ok {
				return next(ctx, req)
			}
			if err := m.Authorize(ctx, action, ResourceRef{}); err != nil {
				return nil, err
			}
			return next(ctx, req)
		}
	}
}
