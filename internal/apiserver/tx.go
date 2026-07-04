package apiserver

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/koriebruh/Jengine/internal/storage/postgres"
	"github.com/koriebruh/Jengine/internal/tenancy"
)

// withTx bridges tenancy.WithTenantTx's tx-as-parameter calling
// convention (the HTTP-request code path - ctx already carries
// TenantContext from WrapAuth) into postgres.ContextWithTx, so a
// handler's callback can call domain repository methods directly
// (they resolve their transaction via postgres.TxFromContext).
func withTx(ctx context.Context, pool *pgxpool.Pool, fn func(ctx context.Context) error) error {
	return tenancy.WithTenantTx(ctx, pool, func(ctx context.Context, tx pgx.Tx) error {
		return fn(postgres.ContextWithTx(ctx, tx))
	})
}
