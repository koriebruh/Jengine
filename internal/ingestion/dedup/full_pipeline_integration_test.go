package dedup_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/koriebruh/Jengine/internal/domain"
	"github.com/koriebruh/Jengine/internal/ingestion/connector"
	"github.com/koriebruh/Jengine/internal/ingestion/dedup"
	"github.com/koriebruh/Jengine/internal/ingestion/mapping"
	"github.com/koriebruh/Jengine/internal/ingestion/parsers/mt940"
	"github.com/koriebruh/Jengine/internal/ingestion/pipeline"
	"github.com/koriebruh/Jengine/internal/ingestion/validation"
	"github.com/koriebruh/Jengine/internal/storage/postgres"
	"github.com/koriebruh/Jengine/internal/tenancy"
	"github.com/koriebruh/Jengine/internal/testutil"
)

type formatParseStage struct{}

func (formatParseStage) Name() string { return "format_parse" }
func (formatParseStage) Process(ctx context.Context, rec *pipeline.PipelineRecord) (pipeline.StageResult, error) {
	if err := json.Unmarshal(rec.Raw.Payload, &rec.ParsedFields); err != nil {
		return pipeline.StageQuarantine, err
	}
	return pipeline.StageContinue, nil
}

// replayConnector emits the SAME fixed slice of RawRecords every time
// Fetch is called - standing in for "the identical batch is replayed"
// (e.g. from Kafka retention/redrive) without conflating this test with
// task 07's SFTP-connector-level Statement-creation-per-poll semantics
// (a separate, already-tested concern - see connector/sftp's own tests).
type replayConnector struct {
	records []connector.RawRecord
}

func (c *replayConnector) Fetch(ctx context.Context, cfg connector.ConnectorConfig) (<-chan connector.RawRecord, error) {
	ch := make(chan connector.RawRecord, len(c.records))
	for _, r := range c.records {
		ch <- r
	}
	close(ch)
	return ch, nil
}
func (c *replayConnector) Validate(cfg connector.ConnectorConfig) error { return nil }
func (c *replayConnector) SupportsStreaming() bool                      { return false }
func (c *replayConnector) Checkpoint() (connector.Cursor, error)        { return connector.Cursor{}, nil }

// TestFullIngestionPipeline_EndToEnd proves plans/task/core/09's
// Definition of Done: task 07's MT940 fixture (parsed once into two
// field_61/field_86 JSON records), mapped via task 08, validated and
// deduped by this task, produces exactly the expected set of Transaction
// rows with correct ingestion_idempotency_key values - and replaying the
// identical batch a second time produces zero additional Transaction
// rows.
func TestFullIngestionPipeline_EndToEnd(t *testing.T) {
	if testing.Short() {
		t.Skip("requires Docker - skipped under -short; run make test-integration")
	}

	const sample = `:20:STMT0001
:25:1234567890
:28C:1
:60F:C240101EUR10000,00
:61:2401020103D250,00NTRFNONREF123
:86:PAYMENT TO SUPPLIER ABC REF:INV1001
:61:240103C500,00NMSCREF456//BANKREF01
:86:INCOMING PAYMENT FROM CUSTOMER XYZ REF:INV1002
:62F:C240103EUR10250,00
-
`
	stmt, err := mt940.Parse([]byte(sample), "generic")
	if err != nil {
		t.Fatalf("parse mt940 fixture: %v", err)
	}
	if len(stmt.Lines) != 2 {
		t.Fatalf("expected 2 parsed lines, got %d", len(stmt.Lines))
	}

	rdb := testutil.StartRedis(t)
	db := testutil.StartPostgres(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	tenantID := uuid.New()
	if _, err := db.Pool.Exec(ctx,
		`INSERT INTO tenants (id, name, isolation_tier, region, status) VALUES ($1, 'Acme', 'STANDARD', 'us-east', 'ACTIVE')`,
		tenantID,
	); err != nil {
		t.Fatalf("seed tenant failed: %v", err)
	}
	accountID := uuid.New()
	if _, err := db.Pool.Exec(ctx,
		`INSERT INTO accounts (id, tenant_id, external_account_ref, account_type, currency, name) VALUES ($1, $2, '1234567890', 'BANK', 'EUR', 'Test Account')`,
		accountID, tenantID,
	); err != nil {
		t.Fatalf("seed account failed: %v", err)
	}
	connectorID := uuid.New()
	if _, err := db.Pool.Exec(ctx,
		`INSERT INTO connectors (id, tenant_id, type, status) VALUES ($1, $2, 'sftp_mt940', 'ACTIVE')`,
		connectorID, tenantID,
	); err != nil {
		t.Fatalf("seed connector failed: %v", err)
	}

	mt940Spec, err := mapping.ParseSpecYAML(mapping.MT940DefaultSpecYAML)
	if err != nil {
		t.Fatalf("parse mt940 default spec: %v", err)
	}
	mt940SpecJSON, err := json.Marshal(mt940Spec)
	if err != nil {
		t.Fatalf("marshal mt940 default spec: %v", err)
	}
	if _, err := db.Pool.Exec(ctx,
		`INSERT INTO mapping_specs (tenant_id, source_format, version, status, spec) VALUES ($1, 'MT940', 1, 'ACTIVE', $2)`,
		tenantID, mt940SpecJSON,
	); err != nil {
		t.Fatalf("seed mapping spec: %v", err)
	}

	appPool := testutil.AppRolePool(t, ctx, db.DSN)
	defer appPool.Close()

	txRunner := func(ctx context.Context, tid uuid.UUID, fn func(ctx context.Context) error) error {
		return postgres.WithTx(tenancy.WithTenant(ctx, tenancy.TenantContext{TenantID: tid}), appPool, tid, fn)
	}

	txRepo := postgres.NewTransactionRepo()
	accountRepo := postgres.NewAccountRepo()
	fxRateRepo := postgres.NewFXRateRepo()
	mappingSpecRepo := postgres.NewMappingSpecRepo()
	dedupRepo := postgres.NewIngestionDedupRepo()
	outboxRepo := postgres.NewOutboxRepo(db.Pool)
	bloom := dedup.NewRedisBloomFilter(rdb.Client, "test:pipeline:bloom", 1000, 0.01)

	// A single fixed BatchID shared by both records and both pipeline
	// runs - simulating "the identical batch is replayed," which is what
	// makes ingestion_idempotency_key identical across runs and proves
	// the dedup guarantee this task builds.
	batchID := uuid.New()
	records := make([]connector.RawRecord, len(stmt.Lines))
	for i, line := range stmt.Lines {
		payload, _ := json.Marshal(map[string]any{"field_61": line.Field61, "field_86": line.Field86})
		records[i] = connector.RawRecord{
			TenantID: tenantID, ConnectorID: connectorID, SourceFormat: "MT940",
			Payload: payload, BatchID: batchID, SourceMode: domain.SourceModeBatch,
		}
	}

	buildPipeline := func() *pipeline.Pipeline {
		return &pipeline.Pipeline{
			Stages: []pipeline.Stage{
				formatParseStage{},
				mapping.NewEngine(mappingSpecRepo, txRunner),
				&mapping.NormalizationStage{
					TenantID: tenantID, AccountID: accountID,
					Accounts: accountRepo, FXRates: fxRateRepo, TxRunner: txRunner,
				},
				&validation.ValidationStage{AccountID: accountID},
				&dedup.DedupStage{
					TenantID: tenantID, ConnectorID: connectorID,
					Bloom: bloom, Transactions: txRepo, Dedup: dedupRepo, TxRunner: txRunner,
				},
				&postgres.PersistEmitStage{
					Pool: appPool, TenantID: tenantID, Outbox: outboxRepo,
					Persist: func(ctx context.Context, rec *pipeline.PipelineRecord) (string, string, []byte, error) {
						n := rec.Normalized
						tx, err := txRepo.Create(ctx, tenantID, domain.Transaction{
							AccountID:   accountID,
							ExternalRef: n.ExternalRef, Amount: n.Amount, Currency: n.Currency,
							BaseAmount: n.BaseAmount, FXRateToBase: &n.FXRateToBase,
							ValueDate: n.ValueDate, BookingDate: n.BookingDate,
							Description: n.Description, Side: n.Side,
							SourceMode:              domain.SourceModeBatch,
							IngestionIdempotencyKey: rec.IdempotencyKey,
							Status:                  domain.TransactionStatusUnmatched,
						})
						if err != nil {
							return "", "", nil, err
						}
						payload, _ := json.Marshal(map[string]any{"transaction_id": tx.ID.String()})
						return "ingestion.raw.test", tx.ID.String(), payload, nil
					},
				},
			},
		}
	}

	cfg := connector.ConnectorConfig{TenantID: tenantID, ConnectorID: connectorID}
	runCtx := tenancy.WithTenant(ctx, tenancy.TenantContext{TenantID: tenantID})

	// --- First run: produces exactly 2 Transaction rows. ---
	if err := buildPipeline().Run(runCtx, &replayConnector{records: records}, cfg); err != nil {
		t.Fatalf("first pipeline run failed: %v", err)
	}

	var count1 int
	if err := db.Pool.QueryRow(ctx, `SELECT count(*) FROM transactions WHERE account_id = $1`, accountID).Scan(&count1); err != nil {
		t.Fatalf("count query failed: %v", err)
	}
	if count1 != 2 {
		t.Fatalf("expected 2 transactions after first run, got %d", count1)
	}

	var keyCount int
	if err := db.Pool.QueryRow(ctx, `SELECT count(DISTINCT ingestion_idempotency_key) FROM transactions WHERE account_id = $1`, accountID).Scan(&keyCount); err != nil {
		t.Fatalf("distinct key count query failed: %v", err)
	}
	if keyCount != 2 {
		t.Fatalf("expected 2 distinct idempotency keys, got %d", keyCount)
	}

	// --- Second run: replay the IDENTICAL batch (same BatchID, same
	// records) - must produce ZERO additional Transaction rows. ---
	if err := buildPipeline().Run(runCtx, &replayConnector{records: records}, cfg); err != nil {
		t.Fatalf("second (replay) pipeline run failed: %v", err)
	}

	var count2 int
	if err := db.Pool.QueryRow(ctx, `SELECT count(*) FROM transactions WHERE account_id = $1`, accountID).Scan(&count2); err != nil {
		t.Fatalf("count query after replay failed: %v", err)
	}
	if count2 != 2 {
		t.Fatalf("expected still 2 transactions after replaying the identical batch, got %d", count2)
	}
}
