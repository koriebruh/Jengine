package apiserver

import (
	"context"
	"fmt"
	"time"

	"connectrpc.com/connect"
	"github.com/jackc/pgx/v5/pgxpool"

	jenginev1 "github.com/koriebruh/Jengine/gen/go/jengine/v1"
	"github.com/koriebruh/Jengine/gen/go/jengine/v1/jenginev1connect"
	"github.com/koriebruh/Jengine/internal/domain"
	"github.com/koriebruh/Jengine/internal/notify"
	"github.com/koriebruh/Jengine/internal/tenancy"
)

// WebhookServiceHandler implements jenginev1connect.WebhookServiceHandler
// (plans/task/core/21) - added independently beside task 15's existing
// services, per this task's own scoping instruction not to touch those.
type WebhookServiceHandler struct {
	Pool              *pgxpool.Pool
	Subscriptions     domain.WebhookSubscriptionRepository
	Deliveries        domain.WebhookDeliveryRepository
	StreamTokenSecret string
}

func subscriptionToProto(s domain.WebhookSubscription) *jenginev1.WebhookSubscription {
	filterExpr := ""
	if s.FilterExpr != nil {
		filterExpr = *s.FilterExpr
	}
	return &jenginev1.WebhookSubscription{
		Id: s.ID.String(), Url: s.URL, SecretRef: s.SecretRef, EventTypes: s.EventTypes,
		FilterExpr: filterExpr, Status: string(s.Status),
		CreatedAt: toTimestamp(s.CreatedAt), UpdatedAt: toTimestamp(s.UpdatedAt),
	}
}

func deliveryToProto(d domain.WebhookDelivery) *jenginev1.WebhookDelivery {
	out := &jenginev1.WebhookDelivery{
		Id: d.ID.String(), SubscriptionId: d.SubscriptionID.String(), EventId: d.EventID,
		EventType: d.EventType, AttemptCount: int32(d.AttemptCount), Status: string(d.Status),
		CreatedAt: toTimestamp(d.CreatedAt),
	}
	if d.LastAttemptAt != nil {
		out.LastAttemptAt = toTimestamp(*d.LastAttemptAt)
	}
	if d.NextAttemptAt != nil {
		out.NextAttemptAt = toTimestamp(*d.NextAttemptAt)
	}
	if d.ResponseStatus != nil {
		out.ResponseStatus = int32(*d.ResponseStatus)
	}
	if d.ResponseBodySnippet != nil {
		out.ResponseBodySnippet = *d.ResponseBodySnippet
	}
	return out
}

func (h *WebhookServiceHandler) CreateSubscription(ctx context.Context, req *connect.Request[jenginev1.CreateSubscriptionRequest]) (*connect.Response[jenginev1.CreateSubscriptionResponse], error) {
	tenantID := tenancy.MustTenantFromContext(ctx).TenantID
	for _, et := range req.Msg.EventTypes {
		if !notify.IsCataloged(et) {
			return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("apiserver: unrecognized event type %q", et))
		}
	}

	var filterExpr *string
	if req.Msg.FilterExpr != "" {
		filterExpr = &req.Msg.FilterExpr
	}

	var created domain.WebhookSubscription
	err := withTx(ctx, h.Pool, func(ctx context.Context) error {
		var err error
		created, err = h.Subscriptions.Create(ctx, tenantID, domain.WebhookSubscription{
			URL: req.Msg.Url, SecretRef: req.Msg.SecretRef, EventTypes: req.Msg.EventTypes, FilterExpr: filterExpr,
			Status: domain.WebhookSubscriptionStatusActive,
		})
		return err
	})
	if err != nil {
		return nil, fmt.Errorf("apiserver: create webhook subscription: %w", err)
	}
	return connect.NewResponse(&jenginev1.CreateSubscriptionResponse{Subscription: subscriptionToProto(created)}), nil
}

func (h *WebhookServiceHandler) ListSubscriptions(ctx context.Context, req *connect.Request[jenginev1.ListSubscriptionsRequest]) (*connect.Response[jenginev1.ListSubscriptionsResponse], error) {
	tenantID := tenancy.MustTenantFromContext(ctx).TenantID

	var subs []domain.WebhookSubscription
	err := withTx(ctx, h.Pool, func(ctx context.Context) error {
		var err error
		subs, err = h.Subscriptions.ListByTenant(ctx, tenantID)
		return err
	})
	if err != nil {
		return nil, fmt.Errorf("apiserver: list webhook subscriptions: %w", err)
	}

	out := make([]*jenginev1.WebhookSubscription, len(subs))
	for i, s := range subs {
		out[i] = subscriptionToProto(s)
	}
	return connect.NewResponse(&jenginev1.ListSubscriptionsResponse{Subscriptions: out}), nil
}

func (h *WebhookServiceHandler) UpdateSubscriptionStatus(ctx context.Context, req *connect.Request[jenginev1.UpdateSubscriptionStatusRequest]) (*connect.Response[jenginev1.UpdateSubscriptionStatusResponse], error) {
	tenantID := tenancy.MustTenantFromContext(ctx).TenantID
	id, err := parseUUID("id", req.Msg.Id)
	if err != nil {
		return nil, err
	}
	status := domain.WebhookSubscriptionStatus(req.Msg.Status)
	switch status {
	case domain.WebhookSubscriptionStatusActive, domain.WebhookSubscriptionStatusPaused, domain.WebhookSubscriptionStatusDisabled:
	default:
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("apiserver: unrecognized subscription status %q", req.Msg.Status))
	}

	var updated domain.WebhookSubscription
	err = withTx(ctx, h.Pool, func(ctx context.Context) error {
		if err := h.Subscriptions.UpdateStatus(ctx, tenantID, id, status); err != nil {
			return err
		}
		var err error
		updated, err = h.Subscriptions.GetByID(ctx, tenantID, id)
		return err
	})
	if err != nil {
		return nil, fmt.Errorf("apiserver: update webhook subscription status: %w", err)
	}
	return connect.NewResponse(&jenginev1.UpdateSubscriptionStatusResponse{Subscription: subscriptionToProto(updated)}), nil
}

func (h *WebhookServiceHandler) ListDeliveries(ctx context.Context, req *connect.Request[jenginev1.ListDeliveriesRequest]) (*connect.Response[jenginev1.ListDeliveriesResponse], error) {
	tenantID := tenancy.MustTenantFromContext(ctx).TenantID
	status := domain.WebhookDeliveryStatus(req.Msg.Status)
	if status == "" {
		status = domain.WebhookDeliveryStatusDeadLettered // ListDeliveries' primary use case is the DLQ view, per this task's own Implementation Notes ("DEAD_LETTERED deliveries are listable via WebhookService.ListDeliveries filtered by status")
	}

	var deliveries []domain.WebhookDelivery
	err := withTx(ctx, h.Pool, func(ctx context.Context) error {
		var err error
		deliveries, err = h.Deliveries.ListByStatus(ctx, tenantID, status)
		return err
	})
	if err != nil {
		return nil, fmt.Errorf("apiserver: list webhook deliveries: %w", err)
	}

	out := make([]*jenginev1.WebhookDelivery, len(deliveries))
	for i, d := range deliveries {
		out[i] = deliveryToProto(d)
	}
	return connect.NewResponse(&jenginev1.ListDeliveriesResponse{Deliveries: out}), nil
}

func (h *WebhookServiceHandler) RedriveDelivery(ctx context.Context, req *connect.Request[jenginev1.RedriveDeliveryRequest]) (*connect.Response[jenginev1.RedriveDeliveryResponse], error) {
	tenantID := tenancy.MustTenantFromContext(ctx).TenantID
	id, err := parseUUID("id", req.Msg.Id)
	if err != nil {
		return nil, err
	}

	var redriven domain.WebhookDelivery
	err = withTx(ctx, h.Pool, func(ctx context.Context) error {
		if err := h.Deliveries.Redrive(ctx, tenantID, id); err != nil {
			return err
		}
		var err error
		redriven, err = h.Deliveries.GetByID(ctx, tenantID, id)
		return err
	})
	if err != nil {
		return nil, fmt.Errorf("apiserver: redrive webhook delivery: %w", err)
	}
	return connect.NewResponse(&jenginev1.RedriveDeliveryResponse{Delivery: deliveryToProto(redriven)}), nil
}

func (h *WebhookServiceHandler) MintStreamToken(ctx context.Context, req *connect.Request[jenginev1.MintStreamTokenRequest]) (*connect.Response[jenginev1.MintStreamTokenResponse], error) {
	tenantID := tenancy.MustTenantFromContext(ctx).TenantID
	token, expiresAt := notify.MintStreamToken(h.StreamTokenSecret, tenantID, time.Now())
	return connect.NewResponse(&jenginev1.MintStreamTokenResponse{Token: token, ExpiresAt: toTimestamp(expiresAt)}), nil
}

var _ jenginev1connect.WebhookServiceHandler = (*WebhookServiceHandler)(nil)
