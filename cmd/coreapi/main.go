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
	"fmt"
	"log"
	"log/slog"
	"net/http"
	"os"
	"time"

	"connectrpc.com/connect"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel"
	temporalclient "go.temporal.io/sdk/client"
	temporalworker "go.temporal.io/sdk/worker"

	"github.com/koriebruh/Jengine/gen/go/jengine/v1/jenginev1connect"
	"github.com/koriebruh/Jengine/internal/apiserver"
	"github.com/koriebruh/Jengine/internal/audit"
	"github.com/koriebruh/Jengine/internal/cases"
	caseworkflow "github.com/koriebruh/Jengine/internal/cases/workflow"
	"github.com/koriebruh/Jengine/internal/ingestion/connector"
	"github.com/koriebruh/Jengine/internal/ingestion/connector/sftp"
	"github.com/koriebruh/Jengine/internal/ingestion/connector/webhookreceiver"
	"github.com/koriebruh/Jengine/internal/matching/rules"
	"github.com/koriebruh/Jengine/internal/platform/authz"
	"github.com/koriebruh/Jengine/internal/platform/observability"
	"github.com/koriebruh/Jengine/internal/platform/outbox"
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

// temporalTaskQueue is the one task queue every case.go workflow/activity
// in this process uses - single tenant-agnostic queue at MVP (per-tenant
// task queues would be a Dedicated-tier isolation lever, not needed yet).
const temporalTaskQueue = "case-lifecycle"

// setupTemporalWorker connects to the local dev Temporal server
// (plans/task/core/02 - provisioned ahead of need, unused until this
// task), registers BreakLifecycleWorkflow/ApprovalWorkflow and their
// Activities, and starts the worker as a goroutine inside this same
// process - plans/task/core/20's own deployment-topology note: no
// separate cmd/case-worker binary, the task-queue poller runs alongside
// the HTTP/Connect-RPC server started later in main().
func setupTemporalWorker(appPool *pgxpool.Pool, txRunner cases.TxRunner, auditWriter audit.Writer, opaClient authz.OPAClient) (*cases.TemporalLifecycleService, error) {
	temporalHostPort := envOrDefault("TEMPORAL_HOSTPORT", "localhost:7233")
	c, err := temporalclient.Dial(temporalclient.Options{HostPort: temporalHostPort})
	if err != nil {
		return nil, err
	}

	insertOutbox := func(ctx context.Context, tenantID uuid.UUID, aggregateID uuid.UUID, eventType, topic string, payload []byte) error {
		tx, ok := postgres.TxFromContext(ctx)
		if !ok {
			return errNoTxInContext
		}
		return outbox.Insert(ctx, tx, tenantID, outbox.Event{
			AggregateType: "case", AggregateID: aggregateID, EventType: eventType, Topic: topic, Payload: payload,
		})
	}

	activities := &caseworkflow.Activities{
		TxRunner: caseworkflow.TxRunner(txRunner), Cases: postgres.NewCaseRepo(), Audit: auditWriter,
		Routing: postgres.NewCaseRoutingConfigRepo(), InsertOutbox: insertOutbox, OPAClient: opaClient,
	}

	w := temporalworker.New(c, temporalTaskQueue, temporalworker.Options{})
	w.RegisterWorkflow(caseworkflow.BreakLifecycleWorkflow)
	w.RegisterWorkflow(caseworkflow.ApprovalWorkflow)
	w.RegisterActivity(activities)

	go func() {
		if err := w.Run(temporalworker.InterruptCh()); err != nil {
			log.Fatalf("coreapi: temporal worker: %v", err)
		}
	}()

	return cases.NewTemporalLifecycleService(c, temporalTaskQueue, txRunner, postgres.NewCaseRepo()), nil
}

var errNoTxInContext = fmt.Errorf("coreapi: no pgx.Tx in context for outbox insert")

func main() {
	ctx := context.Background()

	superuserDSN := envOrDefault("DATABASE_URL", "postgres://jengine:jengine_dev@localhost:5432/jengine?sslmode=disable")
	appDSN := envOrDefault("APP_DATABASE_URL", "postgres://jengine_app:jengine_app_dev@localhost:5432/jengine?sslmode=disable")
	addr := envOrDefault("COREAPI_ADDR", ":8081")
	jwtSecret := envOrDefault("JWT_SECRET", "dev-only-insecure-secret")

	obsCfg := observability.Config{
		ServiceName: "coreapi", ServiceVersion: "dev", Environment: envOrDefault("ENVIRONMENT", "dev"),
		OTLPEndpoint: envOrDefault("OTEL_EXPORTER_OTLP_ENDPOINT", "localhost:4318"),
		MetricsAddr:  envOrDefault("METRICS_ADDR", ":9091"),
	}
	logger := observability.NewLogger(obsCfg)
	slog.SetDefault(logger)

	shutdownTracer, err := observability.InitTracerProvider(ctx, obsCfg)
	if err != nil {
		log.Fatalf("coreapi: init tracer provider: %v", err)
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := shutdownTracer(shutdownCtx); err != nil {
			log.Printf("coreapi: tracer shutdown: %v", err)
		}
	}()

	shutdownMeter, err := observability.InitMeterProvider(ctx, obsCfg)
	if err != nil {
		log.Fatalf("coreapi: init meter provider: %v", err)
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := shutdownMeter(shutdownCtx); err != nil {
			log.Printf("coreapi: meter shutdown: %v", err)
		}
	}()

	metrics, err := observability.NewMetrics(otel.Meter("coreapi"))
	if err != nil {
		log.Fatalf("coreapi: new metrics: %v", err)
	}
	interceptor := observability.NewConnectInterceptor("coreapi", metrics)
	handlerOpts := []connect.HandlerOption{connect.WithInterceptors(interceptor)}

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

	webhookReceiver := webhookreceiver.New(sftp.EnvSecretResolver{})
	if err := loadWebhookConnectors(ctx, superuserPool, webhookReceiver); err != nil {
		log.Fatalf("coreapi: load webhook connectors: %v", err)
	}

	idempotency := apiserver.NewPostgresIdempotencyStore(appPool)
	auditWriter := audit.NewPostgresWriter()

	mux := http.NewServeMux()

	accountHandler := &apiserver.AccountServiceHandler{Pool: appPool, Accounts: postgres.NewAccountRepo(), Idempotency: idempotency}
	mux.Handle(jenginev1connect.NewAccountServiceHandler(accountHandler, handlerOpts...))

	statementHandler := &apiserver.StatementServiceHandler{Pool: appPool, Statements: postgres.NewStatementRepo()}
	mux.Handle(jenginev1connect.NewStatementServiceHandler(statementHandler, handlerOpts...))

	transactionHandler := &apiserver.TransactionServiceHandler{Pool: appPool, Transactions: postgres.NewTransactionRepo()}
	mux.Handle(jenginev1connect.NewTransactionServiceHandler(transactionHandler, handlerOpts...))

	// plans/task/core/23: OPA sidecar (opa run --server, policy bundle
	// from deploy/opa/policies/) - real RBAC/ABAC decisions, not inline
	// Go role checks.
	opaClient := authz.NewHTTPOPAClient(envOrDefault("OPA_URL", "http://localhost:8181"))
	authzMiddleware := authz.NewMiddleware(opaClient)

	matchRuleHandler := &apiserver.MatchRuleServiceHandler{
		Pool: appPool, Rules: postgres.NewMatchRuleRepo(), Registry: rules.DefaultRegistry(), Idempotency: idempotency,
		Authz: authzMiddleware,
	}
	mux.Handle(jenginev1connect.NewMatchRuleServiceHandler(matchRuleHandler, handlerOpts...))

	postgresLifecycle := cases.NewPostgresLifecycleService(newCasesTxRunner(appPool), postgres.NewCaseRepo(), auditWriter)

	temporalLifecycle, err := setupTemporalWorker(appPool, newCasesTxRunner(appPool), auditWriter, opaClient)
	if err != nil {
		log.Fatalf("coreapi: setup temporal worker: %v", err)
	}

	// plans/task/core/20: tenant-by-tenant cutover between task 13's
	// Postgres-only lifecycle and this task's Temporal-backed one, keyed
	// by the cases.temporal_enabled feature flag - see
	// cases.FeatureFlagLifecycleService's own doc comment for why this
	// isn't a single big-bang switch.
	lifecycle := cases.NewFeatureFlagLifecycleService(registry, postgresLifecycle, temporalLifecycle)

	matchReviewHandler := &apiserver.MatchReviewServiceHandler{
		Pool: appPool, MatchResults: postgres.NewMatchResultRepo(), Transactions: postgres.NewTransactionRepo(),
		Lifecycle: lifecycle, Audit: auditWriter, Idempotency: idempotency,
	}
	mux.Handle(jenginev1connect.NewMatchReviewServiceHandler(matchReviewHandler, handlerOpts...))

	breakHandler := &apiserver.BreakServiceHandler{Pool: appPool, Cases: postgres.NewCaseRepo(), Lifecycle: lifecycle, Idempotency: idempotency}
	mux.Handle(jenginev1connect.NewBreakServiceHandler(breakHandler, handlerOpts...))

	// plans/task/core/21: WebhookService added independently beside the
	// services above - not modifying any of their definitions/handlers,
	// per that task's own scoping instruction.
	webhookServiceHandler := &apiserver.WebhookServiceHandler{
		Pool: appPool, Subscriptions: postgres.NewWebhookSubscriptionRepo(), Deliveries: postgres.NewWebhookDeliveryRepo(),
		StreamTokenSecret: envOrDefault("STREAM_TOKEN_SECRET", "dev-only-insecure-stream-token-secret"),
	}
	mux.Handle(jenginev1connect.NewWebhookServiceHandler(webhookServiceHandler, handlerOpts...))

	// Top-level mux: the Connect-RPC API is tenancy.Middleware-gated
	// (JWT/API-key), but plans/task/core/18's webhook-receiver connector
	// is its OWN auth path (per-connector HMAC signature, no tenant JWT -
	// a payment gateway sending a settlement webhook has neither) so it
	// must sit outside WrapAuth, not behind it.
	topMux := http.NewServeMux()
	topMux.HandleFunc("POST /v1/webhooks/ingest/{tenant_id}/{connector_id}", webhookIngestHandler(webhookReceiver))
	topMux.Handle("/", apiserver.WrapAuth(authMiddleware, mux))

	server := &http.Server{
		Addr:    addr,
		Handler: topMux,
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

// loadWebhookConnectors registers every ACTIVE "webhook" type connector
// (plans/task/core/18) with recv at startup, so ServeHTTP can route
// incoming requests by connector ID from process start. A raw query
// here (rather than a new domain.ConnectorRepository method) since
// "list every webhook connector across all tenants" is a startup-only,
// cross-tenant query no other caller needs - domain.ConnectorRepository
// is deliberately tenant-scoped (ListByTenant) everywhere else.
//
// Known gap, not solved here: a webhook connector created AFTER this
// process starts isn't picked up until restart - there's no
// ConnectorService/API endpoint yet (task 15's service list doesn't
// include one) that could call webhookReceiver.Fetch() at creation
// time. Flagged in QA_REPORT.md rather than silently left unnoted.
func loadWebhookConnectors(ctx context.Context, pool *pgxpool.Pool, recv *webhookreceiver.Connector) error {
	rows, err := pool.Query(ctx,
		`SELECT tenant_id, id, config FROM connectors WHERE type = 'webhook' AND status = 'ACTIVE'`)
	if err != nil {
		return err
	}
	defer rows.Close()

	var loaded int
	for rows.Next() {
		var tenantID, connectorID uuid.UUID
		var cfgBytes []byte
		if err := rows.Scan(&tenantID, &connectorID, &cfgBytes); err != nil {
			return err
		}
		_, err := recv.Fetch(ctx, connector.ConnectorConfig{
			TenantID: tenantID, ConnectorID: connectorID, Type: "webhook", Settings: cfgBytes,
		})
		if err != nil {
			return err
		}
		loaded++
	}
	log.Printf("coreapi: loaded %d webhook connector(s)", loaded)
	return rows.Err()
}

// webhookIngestHandler adapts webhookreceiver.Connector.ServeHTTP's
// (w, r, tenantID, connectorID) signature to a standard http.HandlerFunc,
// extracting path values via Go 1.22+'s ServeMux wildcards.
func webhookIngestHandler(recv *webhookreceiver.Connector) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		tenantID, err := uuid.Parse(r.PathValue("tenant_id"))
		if err != nil {
			http.Error(w, "invalid tenant_id", http.StatusBadRequest)
			return
		}
		connectorID, err := uuid.Parse(r.PathValue("connector_id"))
		if err != nil {
			http.Error(w, "invalid connector_id", http.StatusBadRequest)
			return
		}
		recv.ServeHTTP(w, r, tenantID, connectorID)
	}
}
