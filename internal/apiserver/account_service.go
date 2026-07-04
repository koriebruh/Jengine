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

// AccountServiceHandler implements jenginev1connect.AccountServiceHandler.
type AccountServiceHandler struct {
	Pool        *pgxpool.Pool
	Accounts    domain.AccountRepository
	Idempotency IdempotencyStore
}

func accountToProto(a domain.Account) *jenginev1.Account {
	return &jenginev1.Account{
		Id: a.ID.String(), ExternalAccountRef: a.ExternalAccountRef,
		AccountType: string(a.AccountType), Currency: a.Currency, Name: a.Name,
		MetadataJson: string(a.Metadata), CreatedAt: toTimestamp(a.CreatedAt), UpdatedAt: toTimestamp(a.UpdatedAt),
	}
}

func (h *AccountServiceHandler) CreateAccount(ctx context.Context, req *connect.Request[jenginev1.CreateAccountRequest]) (*connect.Response[jenginev1.CreateAccountResponse], error) {
	tenantID := tenancy.MustTenantFromContext(ctx).TenantID
	idemKey := req.Header().Get("Idempotency-Key")
	hash, err := ComputeRequestHash(tenantID, req.Spec().Procedure, req.Msg)
	if err != nil {
		return nil, err
	}

	resp, err := WithIdempotency(ctx, h.Idempotency, tenantID, idemKey, hash,
		func() *jenginev1.CreateAccountResponse { return &jenginev1.CreateAccountResponse{} },
		func(ctx context.Context) (*jenginev1.CreateAccountResponse, error) {
			var created domain.Account
			err := withTx(ctx, h.Pool, func(ctx context.Context) error {
				var err error
				created, err = h.Accounts.Create(ctx, tenantID, domain.Account{
					ExternalAccountRef: req.Msg.ExternalAccountRef,
					AccountType:        domain.AccountType(req.Msg.AccountType),
					Currency:           req.Msg.Currency,
					Name:               req.Msg.Name,
					Metadata:           []byte(req.Msg.MetadataJson),
				})
				return err
			})
			if err != nil {
				return nil, fmt.Errorf("apiserver: create account: %w", err)
			}
			return &jenginev1.CreateAccountResponse{Account: accountToProto(created)}, nil
		})
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(resp), nil
}

func (h *AccountServiceHandler) GetAccount(ctx context.Context, req *connect.Request[jenginev1.GetAccountRequest]) (*connect.Response[jenginev1.GetAccountResponse], error) {
	tenantID := tenancy.MustTenantFromContext(ctx).TenantID
	id, err := parseUUID("id", req.Msg.Id)
	if err != nil {
		return nil, err
	}

	var account domain.Account
	err = withTx(ctx, h.Pool, func(ctx context.Context) error {
		var err error
		account, err = h.Accounts.GetByID(ctx, tenantID, id)
		return err
	})
	if err != nil {
		return nil, fmt.Errorf("apiserver: get account: %w", err)
	}
	return connect.NewResponse(&jenginev1.GetAccountResponse{Account: accountToProto(account)}), nil
}

func (h *AccountServiceHandler) ListAccounts(ctx context.Context, req *connect.Request[jenginev1.ListAccountsRequest]) (*connect.Response[jenginev1.ListAccountsResponse], error) {
	tenantID := tenancy.MustTenantFromContext(ctx).TenantID

	var accounts []domain.Account
	err := withTx(ctx, h.Pool, func(ctx context.Context) error {
		var err error
		accounts, err = h.Accounts.ListByTenant(ctx, tenantID)
		return err
	})
	if err != nil {
		return nil, fmt.Errorf("apiserver: list accounts: %w", err)
	}

	out := make([]*jenginev1.Account, len(accounts))
	for i, a := range accounts {
		out[i] = accountToProto(a)
	}
	return connect.NewResponse(&jenginev1.ListAccountsResponse{Accounts: out}), nil
}

var _ jenginev1connect.AccountServiceHandler = (*AccountServiceHandler)(nil)
