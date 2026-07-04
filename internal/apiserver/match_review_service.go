package apiserver

import (
	"context"
	"fmt"

	"connectrpc.com/connect"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	jenginev1 "github.com/koriebruh/Jengine/gen/go/jengine/v1"
	"github.com/koriebruh/Jengine/gen/go/jengine/v1/jenginev1connect"
	"github.com/koriebruh/Jengine/internal/audit"
	"github.com/koriebruh/Jengine/internal/cases"
	"github.com/koriebruh/Jengine/internal/domain"
	"github.com/koriebruh/Jengine/internal/tenancy"
)

// MatchReviewServiceHandler implements jenginev1connect.MatchReviewServiceHandler.
// RejectMatch routes through the SAME cases.LifecycleService.OpenBreak
// call task 12's BreakSink uses for unmatched residue - never a second,
// divergent break-creation path (plans/task/core/15 Common Pitfalls).
type MatchReviewServiceHandler struct {
	Pool         *pgxpool.Pool
	MatchResults domain.MatchResultRepository
	Transactions domain.TransactionRepository
	Lifecycle    cases.LifecycleService
	Audit        audit.Writer
	Idempotency  IdempotencyStore
}

func matchResultToProto(r domain.MatchResult, lines []domain.MatchResultLine) *jenginev1.SuggestedMatch {
	ruleID := ""
	if r.RuleID != nil {
		ruleID = r.RuleID.String()
	}
	var sourceIDs, targetIDs []string
	for _, l := range lines {
		switch l.Side {
		case domain.MatchResultLineSideSource:
			sourceIDs = append(sourceIDs, l.TransactionID.String())
		case domain.MatchResultLineSideTarget:
			targetIDs = append(targetIDs, l.TransactionID.String())
		}
	}
	return &jenginev1.SuggestedMatch{
		Id: r.ID.String(), RuleId: ruleID, MatchType: string(r.MatchType),
		ConfidenceScore: r.ConfidenceScore.String(), SourceTransactionIds: sourceIDs, TargetTransactionIds: targetIDs,
		MatchedAt: toTimestamp(r.MatchedAt),
	}
}

func (h *MatchReviewServiceHandler) ListSuggestedMatches(ctx context.Context, req *connect.Request[jenginev1.ListSuggestedMatchesRequest]) (*connect.Response[jenginev1.ListSuggestedMatchesResponse], error) {
	tenantID := tenancy.MustTenantFromContext(ctx).TenantID

	var results []domain.MatchResult
	var lineSets [][]domain.MatchResultLine
	err := withTx(ctx, h.Pool, func(ctx context.Context) error {
		var err error
		results, err = h.MatchResults.ListByStatus(ctx, tenantID, domain.MatchResultStatusSuggested)
		if err != nil {
			return err
		}
		lineSets = make([][]domain.MatchResultLine, len(results))
		for i, r := range results {
			_, lines, err := h.MatchResults.GetByID(ctx, tenantID, r.ID)
			if err != nil {
				return err
			}
			lineSets[i] = lines
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("apiserver: list suggested matches: %w", err)
	}

	out := make([]*jenginev1.SuggestedMatch, len(results))
	for i, r := range results {
		out[i] = matchResultToProto(r, lineSets[i])
	}
	return connect.NewResponse(&jenginev1.ListSuggestedMatchesResponse{Matches: out}), nil
}

func (h *MatchReviewServiceHandler) ConfirmMatch(ctx context.Context, req *connect.Request[jenginev1.ConfirmMatchRequest]) (*connect.Response[jenginev1.ConfirmMatchResponse], error) {
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
		func() *jenginev1.ConfirmMatchResponse { return &jenginev1.ConfirmMatchResponse{} },
		func(ctx context.Context) (*jenginev1.ConfirmMatchResponse, error) {
			var result domain.MatchResult
			var lines []domain.MatchResultLine
			err := withTx(ctx, h.Pool, func(ctx context.Context) error {
				confirmedBy := req.Msg.ConfirmedBy
				if err := h.MatchResults.UpdateStatus(ctx, tenantID, id, domain.MatchResultStatusConfirmed, &confirmedBy); err != nil {
					return err
				}
				var err error
				result, lines, err = h.MatchResults.GetByID(ctx, tenantID, id)
				if err != nil {
					return err
				}
				var txIDs []uuid.UUID
				for _, l := range lines {
					txIDs = append(txIDs, l.TransactionID)
				}
				return h.Transactions.BulkUpdateStatus(ctx, tenantID, txIDs, domain.TransactionStatusMatched)
			})
			if err != nil {
				return nil, fmt.Errorf("apiserver: confirm match: %w", err)
			}
			return &jenginev1.ConfirmMatchResponse{Match: matchResultToProto(result, lines)}, nil
		})
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(resp), nil
}

func (h *MatchReviewServiceHandler) RejectMatch(ctx context.Context, req *connect.Request[jenginev1.RejectMatchRequest]) (*connect.Response[jenginev1.RejectMatchResponse], error) {
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
		func() *jenginev1.RejectMatchResponse { return &jenginev1.RejectMatchResponse{} },
		func(ctx context.Context) (*jenginev1.RejectMatchResponse, error) {
			var lines []domain.MatchResultLine
			var allTxIDs []uuid.UUID
			err := withTx(ctx, h.Pool, func(ctx context.Context) error {
				rejectedBy := req.Msg.RejectedBy
				if err := h.MatchResults.UpdateStatus(ctx, tenantID, id, domain.MatchResultStatusRejected, &rejectedBy); err != nil {
					return err
				}
				var err error
				_, lines, err = h.MatchResults.GetByID(ctx, tenantID, id)
				if err != nil {
					return err
				}
				for _, l := range lines {
					allTxIDs = append(allTxIDs, l.TransactionID)
				}
				// Revert to UNMATCHED - a rejected suggestion's
				// transactions are residue again, exactly like task 12's
				// unmatched leftover, which is why the SAME OpenBreak call
				// below is used rather than a bespoke break-creation path.
				return h.Transactions.BulkUpdateStatus(ctx, tenantID, allTxIDs, domain.TransactionStatusUnmatched)
			})
			if err != nil {
				return nil, fmt.Errorf("apiserver: reject match: %w", err)
			}

			if len(allTxIDs) == 0 {
				return &jenginev1.RejectMatchResponse{}, nil
			}
			// AccountID: the first line's transaction's account - a
			// rejected suggestion's lines may span source+target
			// accounts; picking the first is an MVP simplification
			// (documented) rather than opening multiple breaks per
			// rejection.
			var firstTx domain.Transaction
			if err := withTx(ctx, h.Pool, func(ctx context.Context) error {
				var err error
				firstTx, err = h.Transactions.GetByID(ctx, tenantID, allTxIDs[0])
				return err
			}); err != nil {
				return nil, fmt.Errorf("apiserver: reject match: load account for break: %w", err)
			}

			brk, err := h.Lifecycle.OpenBreak(ctx, cases.OpenBreakParams{
				TenantID: tenantID, AccountID: firstTx.AccountID, TransactionIDs: allTxIDs,
				BreakType: "UNMATCHED", AmountAtRisk: firstTx.BaseAmount, Currency: firstTx.Currency,
			})
			if err != nil {
				return nil, fmt.Errorf("apiserver: reject match: open break: %w", err)
			}
			return &jenginev1.RejectMatchResponse{BreakId: brk.ID.String()}, nil
		})
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(resp), nil
}

func (h *MatchReviewServiceHandler) BulkConfirmMatches(ctx context.Context, req *connect.Request[jenginev1.BulkConfirmMatchesRequest]) (*connect.Response[jenginev1.BulkConfirmMatchesResponse], error) {
	tenantID := tenancy.MustTenantFromContext(ctx).TenantID
	idemKey := req.Header().Get("Idempotency-Key")
	hash, err := ComputeRequestHash(tenantID, req.Spec().Procedure, req.Msg)
	if err != nil {
		return nil, err
	}

	resp, err := WithIdempotency(ctx, h.Idempotency, tenantID, idemKey, hash,
		func() *jenginev1.BulkConfirmMatchesResponse { return &jenginev1.BulkConfirmMatchesResponse{} },
		func(ctx context.Context) (*jenginev1.BulkConfirmMatchesResponse, error) {
			result := &jenginev1.BulkResult{BatchOpId: uuid.New().String(), Failed: make(map[string]string)}

			err := withTx(ctx, h.Pool, func(ctx context.Context) error {
				for _, idStr := range req.Msg.Ids {
					id, err := uuid.Parse(idStr)
					if err != nil {
						result.Failed[idStr] = err.Error()
						continue
					}
					confirmedBy := req.Msg.ConfirmedBy
					if err := h.MatchResults.UpdateStatus(ctx, tenantID, id, domain.MatchResultStatusConfirmed, &confirmedBy); err != nil {
						result.Failed[idStr] = err.Error()
						continue
					}
					_, lines, err := h.MatchResults.GetByID(ctx, tenantID, id)
					if err != nil {
						result.Failed[idStr] = err.Error()
						continue
					}
					var txIDs []uuid.UUID
					for _, l := range lines {
						txIDs = append(txIDs, l.TransactionID)
					}
					if err := h.Transactions.BulkUpdateStatus(ctx, tenantID, txIDs, domain.TransactionStatusMatched); err != nil {
						result.Failed[idStr] = err.Error()
						continue
					}
					result.Succeeded = append(result.Succeeded, idStr)
				}

				if len(result.Succeeded) == 0 {
					return nil
				}
				// Exactly ONE audit event for the whole batch, not one
				// per match result - mirrors cases.PostgresLifecycleService's
				// bulk-operation pattern (plans/docs/05-case-management.md
				// §6.2). MatchResultRepository.UpdateStatus doesn't write
				// its own audit event, so this handler is the right (and
				// only) place to do it - not a double-write. Written
				// inside the same transaction as the status updates above
				// (audit.Writer requires an ambient transaction).
				payload, err := jsonMarshal(map[string]any{"batch_op_id": result.BatchOpId, "succeeded": result.Succeeded, "failed_count": len(result.Failed)})
				if err != nil {
					return err
				}
				return h.Audit.Write(ctx, audit.AuditEvent{
					TenantID: tenantID, ActorID: req.Msg.ConfirmedBy, ActorType: "USER",
					EventType: "match.bulk_confirmed", EntityType: "MatchResult", EntityID: result.BatchOpId,
					AfterState: payload,
				})
			})
			if err != nil {
				return nil, fmt.Errorf("apiserver: bulk confirm matches: %w", err)
			}
			return &jenginev1.BulkConfirmMatchesResponse{Result: result}, nil
		})
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(resp), nil
}

var _ jenginev1connect.MatchReviewServiceHandler = (*MatchReviewServiceHandler)(nil)
