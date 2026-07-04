// Command coreapi serves plans/task/core/15's MVP Connect-RPC API:
// AccountService, StatementService, TransactionService, MatchRuleService,
// MatchReviewService, and BreakService, all behind tenancy.Middleware's
// JWT/API-key auth resolution. Connect-RPC serves gRPC, gRPC-Web, and
// plain JSON-over-HTTP from the same handlers - a plain `curl` with
// `Content-Type: application/json` is a fully valid client, no gRPC
// tooling required.
package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/koriebruh/Jengine/gen/go/jengine/v1/jenginev1connect"
	"github.com/koriebruh/Jengine/internal/apiserver"
	"github.com/koriebruh/Jengine/internal/audit"
	"github.com/koriebruh/Jengine/internal/cases"
	"github.com/koriebruh/Jengine/internal/matching/rules"
	"github.com/koriebruh/Jengine/internal/storage/postgres"
	"github.com/koriebruh/Jengine/internal/tenancy"
)

func envOrDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// newCasesTxRunner builds a cases.TxRunner wrapping postgres.WithTx - the
// same closure shape used by every other binary in this codebase
// (cmd/matching-batch's newTxRunner, cmd/ingestion-gateway's newTxRunner).
func newCasesTxRunner(pool *pgxpool.Pool) cases.TxRunner {
	return func(ctx context.Context, tenantID uuid.UUID, fn func(ctx context.Context) error) error {
		return postgres.WithTx(tenancy.WithTenant(ctx, tenancy.TenantContext{TenantID: tenantID}), pool, tenantID, fn)
	}
}

func main() {
	ctx := context.Background()

	superuserDSN := envOrDefault("DATABASE_URL", "postgres://jengine:jengine_dev@localhost:5432/jengine?sslmode=disable")
	appDSN := envOrDefault("APP_DATABASE_URL", "postgres://jengine_app:jengine_app_dev@localhost:5432/jengine?sslmode=disable")
	addr := envOrDefault("COREAPI_ADDR", ":8081")
	jwtSecret := envOrDefault("JWT_SECRET", "dev-only-insecure-secret")

	superuserPool, err := pgxpool.New(ctx, superuserDSN)
	if err != nil {
		log.Fatalf("coreapi: connect as superuser: %v", err)
	}
	defer superuserPool.Close()

	appPool, err := pgxpool.New(ctx, appDSN)
	if err != nil {
		log.Fatalf("coreapi: connect as jengine_app: %v", err)
	}
	defer appPool.Close()

	registry := tenancy.NewPostgresRegistryRepo(superuserPool)
	authMiddleware := tenancy.NewMiddleware(registry, []byte(jwtSecret))

	idempotency := apiserver.NewPostgresIdempotencyStore(appPool)
	auditWriter := audit.NewPostgresWriter()

	mux := http.NewServeMux()

	accountHandler := &apiserver.AccountServiceHandler{Pool: appPool, Accounts: postgres.NewAccountRepo(), Idempotency: idempotency}
	mux.Handle(jenginev1connect.NewAccountServiceHandler(accountHandler))

	statementHandler := &apiserver.StatementServiceHandler{Pool: appPool, Statements: postgres.NewStatementRepo()}
	mux.Handle(jenginev1connect.NewStatementServiceHandler(statementHandler))

	transactionHandler := &apiserver.TransactionServiceHandler{Pool: appPool, Transactions: postgres.NewTransactionRepo()}
	mux.Handle(jenginev1connect.NewTransactionServiceHandler(transactionHandler))

	matchRuleHandler := &apiserver.MatchRuleServiceHandler{
		Pool: appPool, Rules: postgres.NewMatchRuleRepo(), Registry: rules.DefaultRegistry(), Idempotency: idempotency,
	}
	mux.Handle(jenginev1connect.NewMatchRuleServiceHandler(matchRuleHandler))

	lifecycle := cases.NewPostgresLifecycleService(newCasesTxRunner(appPool), postgres.NewCaseRepo(), auditWriter)

	matchReviewHandler := &apiserver.MatchReviewServiceHandler{
		Pool: appPool, MatchResults: postgres.NewMatchResultRepo(), Transactions: postgres.NewTransactionRepo(),
		Lifecycle: lifecycle, Audit: auditWriter, Idempotency: idempotency,
	}
	mux.Handle(jenginev1connect.NewMatchReviewServiceHandler(matchReviewHandler))

	breakHandler := &apiserver.BreakServiceHandler{Pool: appPool, Cases: postgres.NewCaseRepo(), Lifecycle: lifecycle, Idempotency: idempotency}
	mux.Handle(jenginev1connect.NewBreakServiceHandler(breakHandler))

	handler := apiserver.WrapAuth(authMiddleware, mux)

	server := &http.Server{
		Addr:    addr,
		Handler: handler,
		// Unencrypted HTTP/2 (gRPC needs HTTP/2) alongside HTTP/1.1 (plain
		// JSON/REST clients) on the same plaintext listener - the current
		// (non-deprecated) net/http mechanism for what h2c used to
		// provide as a separate package.
		Protocols:         &http.Protocols{},
		ReadHeaderTimeout: 10 * time.Second,
	}
	server.Protocols.SetUnencryptedHTTP2(true)
	server.Protocols.SetHTTP1(true)

	log.Printf("coreapi: listening on %s", addr)
	if err := server.ListenAndServe(); err != nil {
		log.Fatalf("coreapi: serve: %v", err)
	}
}
