package apiserver

import (
	"context"
	"fmt"

	"connectrpc.com/connect"
	"github.com/jackc/pgx/v5/pgxpool"

	jenginev1 "github.com/koriebruh/Jengine/gen/go/jengine/v1"
	"github.com/koriebruh/Jengine/gen/go/jengine/v1/jenginev1connect"
	"github.com/koriebruh/Jengine/internal/domain"
	"github.com/koriebruh/Jengine/internal/tenancy"
)

// TransactionServiceHandler implements jenginev1connect.TransactionServiceHandler -
// read-only for MVP; no direct transaction mutation via API
// (plans/task/core/15 Implementation Notes).
type TransactionServiceHandler struct {
	Pool         *pgxpool.Pool
	Transactions domain.TransactionRepository
}

func transactionToProto(t domain.Transaction) *jenginev1.Transaction {
	return &jenginev1.Transaction{
		Id: t.ID.String(), AccountId: t.AccountID.String(), ExternalRef: t.ExternalRef,
		Amount: t.Amount.String(), Currency: t.Currency, BaseAmount: t.BaseAmount.String(),
		ValueDate: toTimestamp(t.ValueDate), BookingDate: toTimestamp(t.BookingDate),
		Description: t.Description, CounterpartyRef: t.CounterpartyRef,
		Side: string(t.Side), Status: string(t.Status), CreatedAt: toTimestamp(t.CreatedAt),
	}
}

func (h *TransactionServiceHandler) GetTransaction(ctx context.Context, req *connect.Request[jenginev1.GetTransactionRequest]) (*connect.Response[jenginev1.GetTransactionResponse], error) {
	tenantID := tenancy.MustTenantFromContext(ctx).TenantID
	id, err := parseUUID("id", req.Msg.Id)
	if err != nil {
		return nil, err
	}

	var tx domain.Transaction
	err = withTx(ctx, h.Pool, func(ctx context.Context) error {
		var err error
		tx, err = h.Transactions.GetByID(ctx, tenantID, id)
		return err
	})
	if err != nil {
		return nil, fmt.Errorf("apiserver: get transaction: %w", err)
	}
	return connect.NewResponse(&jenginev1.GetTransactionResponse{Transaction: transactionToProto(tx)}), nil
}

func (h *TransactionServiceHandler) ListTransactions(ctx context.Context, req *connect.Request[jenginev1.ListTransactionsRequest]) (*connect.Response[jenginev1.ListTransactionsResponse], error) {
	tenantID := tenancy.MustTenantFromContext(ctx).TenantID
	accountID, err := parseUUID("account_id", req.Msg.AccountId)
	if err != nil {
		return nil, err
	}

	var transactions []domain.Transaction
	err = withTx(ctx, h.Pool, func(ctx context.Context) error {
		var err error
		transactions, err = h.Transactions.ListByFilter(ctx, tenantID, accountID,
			domain.TransactionStatus(req.Msg.Status), fromTimestamp(req.Msg.ValueDateFrom), fromTimestamp(req.Msg.ValueDateTo))
		return err
	})
	if err != nil {
		return nil, fmt.Errorf("apiserver: list transactions: %w", err)
	}

	out := make([]*jenginev1.Transaction, len(transactions))
	for i, t := range transactions {
		out[i] = transactionToProto(t)
	}
	return connect.NewResponse(&jenginev1.ListTransactionsResponse{Transactions: out}), nil
}

var _ jenginev1connect.TransactionServiceHandler = (*TransactionServiceHandler)(nil)
