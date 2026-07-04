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

// StatementServiceHandler implements jenginev1connect.StatementServiceHandler -
// read-only; statements are created by ingestion, not this API
// (plans/task/core/15 Implementation Notes).
type StatementServiceHandler struct {
	Pool       *pgxpool.Pool
	Statements domain.StatementRepository
}

func statementToProto(s domain.Statement) *jenginev1.Statement {
	sourceConnectorID := ""
	if s.SourceConnectorID != nil {
		sourceConnectorID = s.SourceConnectorID.String()
	}
	return &jenginev1.Statement{
		Id: s.ID.String(), AccountId: s.AccountID.String(), SourceConnectorId: sourceConnectorID,
		Format: s.Format, ReceivedAt: toTimestamp(s.ReceivedAt),
		PeriodStart: toTimestamp(s.PeriodStart), PeriodEnd: toTimestamp(s.PeriodEnd),
		OpeningBalance: s.OpeningBalance.String(), ClosingBalance: s.ClosingBalance.String(),
		Status: string(s.Status), Checksum: s.Checksum, CreatedAt: toTimestamp(s.CreatedAt),
	}
}

func (h *StatementServiceHandler) GetStatement(ctx context.Context, req *connect.Request[jenginev1.GetStatementRequest]) (*connect.Response[jenginev1.GetStatementResponse], error) {
	tenantID := tenancy.MustTenantFromContext(ctx).TenantID
	id, err := parseUUID("id", req.Msg.Id)
	if err != nil {
		return nil, err
	}

	var statement domain.Statement
	err = withTx(ctx, h.Pool, func(ctx context.Context) error {
		var err error
		statement, err = h.Statements.GetByID(ctx, tenantID, id)
		return err
	})
	if err != nil {
		return nil, fmt.Errorf("apiserver: get statement: %w", err)
	}
	return connect.NewResponse(&jenginev1.GetStatementResponse{Statement: statementToProto(statement)}), nil
}

func (h *StatementServiceHandler) ListStatements(ctx context.Context, req *connect.Request[jenginev1.ListStatementsRequest]) (*connect.Response[jenginev1.ListStatementsResponse], error) {
	tenantID := tenancy.MustTenantFromContext(ctx).TenantID
	accountID, err := parseUUID("account_id", req.Msg.AccountId)
	if err != nil {
		return nil, err
	}

	var statements []domain.Statement
	err = withTx(ctx, h.Pool, func(ctx context.Context) error {
		var err error
		statements, err = h.Statements.ListByAccount(ctx, tenantID, accountID)
		return err
	})
	if err != nil {
		return nil, fmt.Errorf("apiserver: list statements: %w", err)
	}

	out := make([]*jenginev1.Statement, len(statements))
	for i, s := range statements {
		out[i] = statementToProto(s)
	}
	return connect.NewResponse(&jenginev1.ListStatementsResponse{Statements: out}), nil
}

var _ jenginev1connect.StatementServiceHandler = (*StatementServiceHandler)(nil)
