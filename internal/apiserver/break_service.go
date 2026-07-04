package apiserver

import (
	"context"
	"fmt"
	"time"

	"connectrpc.com/connect"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	jenginev1 "github.com/koriebruh/Jengine/gen/go/jengine/v1"
	"github.com/koriebruh/Jengine/gen/go/jengine/v1/jenginev1connect"
	"github.com/koriebruh/Jengine/internal/cases"
	"github.com/koriebruh/Jengine/internal/domain"
	"github.com/koriebruh/Jengine/internal/tenancy"
)

// BreakServiceHandler implements jenginev1connect.BreakServiceHandler -
// every mutating RPC is a thin wrapper over cases.LifecycleService
// (plans/task/core/15 Implementation Notes); ListBreaks/GetBreak read
// domain.CaseRepository directly since a pure read has nothing for
// LifecycleService to own.
type BreakServiceHandler struct {
	Pool        *pgxpool.Pool
	Cases       domain.CaseRepository
	Lifecycle   cases.LifecycleService
	Idempotency IdempotencyStore
}

func breakToProto(c domain.Case) *jenginev1.Break {
	rootCause, assignedTo, currency := "", "", ""
	if c.RootCauseCategory != nil {
		rootCause = *c.RootCauseCategory
	}
	if c.AssignedTo != nil {
		assignedTo = *c.AssignedTo
	}
	if c.Currency != nil {
		currency = *c.Currency
	}
	amountAtRisk := ""
	if c.AmountAtRisk != nil {
		amountAtRisk = c.AmountAtRisk.String()
	}
	relatedIDs := make([]string, len(c.RelatedTransactionIDs))
	for i, id := range c.RelatedTransactionIDs {
		relatedIDs[i] = id.String()
	}
	var slaDueAt, resolvedAt time.Time
	if c.SLADueAt != nil {
		slaDueAt = *c.SLADueAt
	}
	if c.ResolvedAt != nil {
		resolvedAt = *c.ResolvedAt
	}
	return &jenginev1.Break{
		Id: c.ID.String(), AccountId: c.AccountID.String(), RelatedTransactionIds: relatedIDs,
		BreakType: string(c.BreakType), RootCauseCategory: rootCause, Status: string(c.Status),
		AssignedTo: assignedTo, Priority: c.Priority, SlaDueAt: toTimestamp(slaDueAt),
		OpenedAt: toTimestamp(c.OpenedAt), ResolvedAt: toTimestamp(resolvedAt),
		AmountAtRisk: amountAtRisk, Currency: currency,
	}
}

func actorFromProto(a *jenginev1.Actor) cases.Actor {
	if a == nil {
		return cases.Actor{}
	}
	return cases.Actor{UserID: a.UserId, Role: a.Role}
}

func (h *BreakServiceHandler) ListBreaks(ctx context.Context, req *connect.Request[jenginev1.ListBreaksRequest]) (*connect.Response[jenginev1.ListBreaksResponse], error) {
	tenantID := tenancy.MustTenantFromContext(ctx).TenantID

	var breaks []domain.Case
	err := withTx(ctx, h.Pool, func(ctx context.Context) error {
		var err error
		breaks, err = h.Cases.ListByStatus(ctx, tenantID, domain.CaseStatus(req.Msg.Status))
		return err
	})
	if err != nil {
		return nil, fmt.Errorf("apiserver: list breaks: %w", err)
	}

	out := make([]*jenginev1.Break, 0, len(breaks))
	for _, c := range breaks {
		if req.Msg.AssignedTo != "" && (c.AssignedTo == nil || *c.AssignedTo != req.Msg.AssignedTo) {
			continue
		}
		if req.Msg.AccountId != "" && c.AccountID.String() != req.Msg.AccountId {
			continue
		}
		if req.Msg.Priority != "" && c.Priority != req.Msg.Priority {
			continue
		}
		out = append(out, breakToProto(c))
	}
	return connect.NewResponse(&jenginev1.ListBreaksResponse{Breaks: out}), nil
}

func (h *BreakServiceHandler) GetBreak(ctx context.Context, req *connect.Request[jenginev1.GetBreakRequest]) (*connect.Response[jenginev1.GetBreakResponse], error) {
	tenantID := tenancy.MustTenantFromContext(ctx).TenantID
	id, err := parseUUID("id", req.Msg.Id)
	if err != nil {
		return nil, err
	}

	var brk domain.Case
	err = withTx(ctx, h.Pool, func(ctx context.Context) error {
		var err error
		brk, err = h.Cases.GetByID(ctx, tenantID, id)
		return err
	})
	if err != nil {
		return nil, fmt.Errorf("apiserver: get break: %w", err)
	}
	return connect.NewResponse(&jenginev1.GetBreakResponse{Brk: breakToProto(brk)}), nil
}

func (h *BreakServiceHandler) AssignBreak(ctx context.Context, req *connect.Request[jenginev1.AssignBreakRequest]) (*connect.Response[jenginev1.AssignBreakResponse], error) {
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
		func() *jenginev1.AssignBreakResponse { return &jenginev1.AssignBreakResponse{} },
		func(ctx context.Context) (*jenginev1.AssignBreakResponse, error) {
			if err := h.Lifecycle.Assign(ctx, id, req.Msg.Assignee, actorFromProto(req.Msg.Actor)); err != nil {
				return nil, err
			}
			return &jenginev1.AssignBreakResponse{}, nil
		})
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(resp), nil
}

func (h *BreakServiceHandler) AddComment(ctx context.Context, req *connect.Request[jenginev1.AddCommentRequest]) (*connect.Response[jenginev1.AddCommentResponse], error) {
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
		func() *jenginev1.AddCommentResponse { return &jenginev1.AddCommentResponse{} },
		func(ctx context.Context) (*jenginev1.AddCommentResponse, error) {
			comment, err := h.Lifecycle.AddComment(ctx, id, actorFromProto(req.Msg.Actor), req.Msg.Body)
			if err != nil {
				return nil, err
			}
			return &jenginev1.AddCommentResponse{CommentId: comment.ID.String()}, nil
		})
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(resp), nil
}

func (h *BreakServiceHandler) TransitionBreak(ctx context.Context, req *connect.Request[jenginev1.TransitionBreakRequest]) (*connect.Response[jenginev1.TransitionBreakResponse], error) {
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
		func() *jenginev1.TransitionBreakResponse { return &jenginev1.TransitionBreakResponse{} },
		func(ctx context.Context) (*jenginev1.TransitionBreakResponse, error) {
			if err := h.Lifecycle.Transition(ctx, id, cases.BreakStatus(req.Msg.ToStatus), actorFromProto(req.Msg.Actor), req.Msg.Comment); err != nil {
				return nil, err
			}
			return &jenginev1.TransitionBreakResponse{}, nil
		})
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(resp), nil
}

func (h *BreakServiceHandler) RequestApproval(ctx context.Context, req *connect.Request[jenginev1.RequestApprovalRequest]) (*connect.Response[jenginev1.RequestApprovalResponse], error) {
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
		func() *jenginev1.RequestApprovalResponse { return &jenginev1.RequestApprovalResponse{} },
		func(ctx context.Context) (*jenginev1.RequestApprovalResponse, error) {
			if err := h.Lifecycle.RequestApproval(ctx, id, actorFromProto(req.Msg.Actor)); err != nil {
				return nil, err
			}
			return &jenginev1.RequestApprovalResponse{}, nil
		})
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(resp), nil
}

func (h *BreakServiceHandler) DecideApproval(ctx context.Context, req *connect.Request[jenginev1.DecideApprovalRequest]) (*connect.Response[jenginev1.DecideApprovalResponse], error) {
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
		func() *jenginev1.DecideApprovalResponse { return &jenginev1.DecideApprovalResponse{} },
		func(ctx context.Context) (*jenginev1.DecideApprovalResponse, error) {
			if err := h.Lifecycle.DecideApproval(ctx, id, actorFromProto(req.Msg.Approver), req.Msg.Approve, req.Msg.Comment); err != nil {
				return nil, err
			}
			return &jenginev1.DecideApprovalResponse{}, nil
		})
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(resp), nil
}

func (h *BreakServiceHandler) TagRootCause(ctx context.Context, req *connect.Request[jenginev1.TagRootCauseRequest]) (*connect.Response[jenginev1.TagRootCauseResponse], error) {
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
		func() *jenginev1.TagRootCauseResponse { return &jenginev1.TagRootCauseResponse{} },
		func(ctx context.Context) (*jenginev1.TagRootCauseResponse, error) {
			if err := h.Lifecycle.TagRootCause(ctx, id, req.Msg.Category, actorFromProto(req.Msg.Actor)); err != nil {
				return nil, err
			}
			return &jenginev1.TagRootCauseResponse{}, nil
		})
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(resp), nil
}

func toProtoBulkResult(r cases.BulkResult) *jenginev1.BulkResult {
	succeeded := make([]string, len(r.Succeeded))
	for i, id := range r.Succeeded {
		succeeded[i] = id.String()
	}
	failed := make(map[string]string, len(r.Failed))
	for id, reason := range r.Failed {
		failed[id.String()] = reason
	}
	return &jenginev1.BulkResult{BatchOpId: r.BatchOpID.String(), Succeeded: succeeded, Failed: failed}
}

func parseUUIDs(ids []string) ([]uuid.UUID, error) {
	out := make([]uuid.UUID, len(ids))
	for i, s := range ids {
		id, err := parseUUID("ids", s)
		if err != nil {
			return nil, err
		}
		out[i] = id
	}
	return out, nil
}

func (h *BreakServiceHandler) BulkAssignBreaks(ctx context.Context, req *connect.Request[jenginev1.BulkAssignBreaksRequest]) (*connect.Response[jenginev1.BulkAssignBreaksResponse], error) {
	tenantID := tenancy.MustTenantFromContext(ctx).TenantID
	idemKey := req.Header().Get("Idempotency-Key")
	hash, err := ComputeRequestHash(tenantID, req.Spec().Procedure, req.Msg)
	if err != nil {
		return nil, err
	}
	ids, err := parseUUIDs(req.Msg.Ids)
	if err != nil {
		return nil, err
	}

	resp, err := WithIdempotency(ctx, h.Idempotency, tenantID, idemKey, hash,
		func() *jenginev1.BulkAssignBreaksResponse { return &jenginev1.BulkAssignBreaksResponse{} },
		func(ctx context.Context) (*jenginev1.BulkAssignBreaksResponse, error) {
			result, err := h.Lifecycle.BulkAssign(ctx, ids, req.Msg.Assignee, actorFromProto(req.Msg.Actor))
			if err != nil {
				return nil, err
			}
			return &jenginev1.BulkAssignBreaksResponse{Result: toProtoBulkResult(result)}, nil
		})
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(resp), nil
}

func (h *BreakServiceHandler) BulkCommentBreaks(ctx context.Context, req *connect.Request[jenginev1.BulkCommentBreaksRequest]) (*connect.Response[jenginev1.BulkCommentBreaksResponse], error) {
	tenantID := tenancy.MustTenantFromContext(ctx).TenantID
	idemKey := req.Header().Get("Idempotency-Key")
	hash, err := ComputeRequestHash(tenantID, req.Spec().Procedure, req.Msg)
	if err != nil {
		return nil, err
	}
	ids, err := parseUUIDs(req.Msg.Ids)
	if err != nil {
		return nil, err
	}

	resp, err := WithIdempotency(ctx, h.Idempotency, tenantID, idemKey, hash,
		func() *jenginev1.BulkCommentBreaksResponse { return &jenginev1.BulkCommentBreaksResponse{} },
		func(ctx context.Context) (*jenginev1.BulkCommentBreaksResponse, error) {
			result, err := h.Lifecycle.BulkAddComment(ctx, ids, actorFromProto(req.Msg.Actor), req.Msg.Body)
			if err != nil {
				return nil, err
			}
			return &jenginev1.BulkCommentBreaksResponse{Result: toProtoBulkResult(result)}, nil
		})
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(resp), nil
}

func (h *BreakServiceHandler) BulkResolveBreaks(ctx context.Context, req *connect.Request[jenginev1.BulkResolveBreaksRequest]) (*connect.Response[jenginev1.BulkResolveBreaksResponse], error) {
	tenantID := tenancy.MustTenantFromContext(ctx).TenantID
	idemKey := req.Header().Get("Idempotency-Key")
	hash, err := ComputeRequestHash(tenantID, req.Spec().Procedure, req.Msg)
	if err != nil {
		return nil, err
	}
	ids, err := parseUUIDs(req.Msg.Ids)
	if err != nil {
		return nil, err
	}

	resp, err := WithIdempotency(ctx, h.Idempotency, tenantID, idemKey, hash,
		func() *jenginev1.BulkResolveBreaksResponse { return &jenginev1.BulkResolveBreaksResponse{} },
		func(ctx context.Context) (*jenginev1.BulkResolveBreaksResponse, error) {
			result, err := h.Lifecycle.BulkTransition(ctx, ids, cases.BreakResolved, actorFromProto(req.Msg.Actor), req.Msg.Comment)
			if err != nil {
				return nil, err
			}
			return &jenginev1.BulkResolveBreaksResponse{Result: toProtoBulkResult(result)}, nil
		})
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(resp), nil
}

var _ jenginev1connect.BreakServiceHandler = (*BreakServiceHandler)(nil)
