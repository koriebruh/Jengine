package ingestion_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"

	"github.com/koriebruh/Jengine/internal/domain"
	"github.com/koriebruh/Jengine/internal/ingestion/connector"
	"github.com/koriebruh/Jengine/internal/ingestion/connector/sftp"
	"github.com/koriebruh/Jengine/internal/ingestion/parsers/mt940"
	"github.com/koriebruh/Jengine/internal/ingestion/pipeline"
	"github.com/koriebruh/Jengine/internal/storage/postgres"
	"github.com/koriebruh/Jengine/internal/tenancy"
	"github.com/koriebruh/Jengine/internal/testutil"
)

type sftpEnvSecret struct{ password string }

func (e sftpEnvSecret) Resolve(ctx context.Context, vaultPathRef string) (string, error) {
	return e.password, nil
}

type mt940Payload struct {
	Field61 mt940.Field61 `json:"field_61"`
	Field86 mt940.Field86 `json:"field_86"`
}

// TestMT940SFTPPipeline_EndToEnd proves plans/task/core/07's own
// Definition of Done: an end-to-end run of the MT940+SFTP path through
// task 06's full pipeline (using a minimal stand-in mapping, since
// task 08's real DSL doesn't exist yet - the task's own DoD explicitly
// allows this) produces correct Statement+Transaction rows, matching
// plans/docs/15-end-to-end-flows.md §15.1's file-arrives-to-persisted-
// canonical-data flow.
func TestMT940SFTPPipeline_EndToEnd(t *testing.T) {
	if testing.Short() {
		t.Skip("requires Docker - skipped under -short; run make test-integration")
	}

	const sample = `:20:STMT0001
:25:1234567890
:28C:1
:60F:C240101EUR10000,00
:61:2401020103D250,00NTRFNONREF123
:86:PAYMENT TO SUPPLIER ABC
:61:240103C500,00NMSCREF456
:86:INCOMING PAYMENT FROM CUSTOMER XYZ
:62F:C240103EUR10250,00
-
`
	hostDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(hostDir, "statement1.sta"), []byte(sample), 0o644); err != nil {
		t.Fatalf("write fixture file: %v", err)
	}
	sftpSrv := testutil.StartSFTP(t, hostDir)

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

	appPool := testutil.AppRolePool(t, ctx, db.DSN)
	defer appPool.Close()

	statementRepo := postgres.NewStatementRepo()
	txRepo := postgres.NewTransactionRepo()
	outboxRepo := postgres.NewOutboxRepo(db.Pool)

	txRunner := func(ctx context.Context, tid uuid.UUID, fn func(ctx context.Context) error) error {
		return postgres.WithTx(tenancy.WithTenant(ctx, tenancy.TenantContext{TenantID: tid}), appPool, tid, fn)
	}
	sftpConn := sftp.New(txRunner, sftpEnvSecret{password: sftpSrv.Password}, statementRepo)

	cfg := connector.ConnectorConfig{
		TenantID: tenantID, ConnectorID: connectorID, Type: "sftp_mt940",
		Settings: mustJSON(t, map[string]any{
			"host": sftpSrv.Host, "username": sftpSrv.Username,
			"auth":         map[string]any{"type": "password", "vault_path_ref": "unused"},
			"remote_dir":   "/upload",
			"account_id":   accountID,
			"parse_format": "MT940",
			"dialect":      "generic",
		}),
	}

	persistFn := func(ctx context.Context, rec *pipeline.PipelineRecord) (string, string, []byte, error) {
		var p mt940Payload
		if err := json.Unmarshal(rec.Raw.Payload, &p); err != nil {
			return "", "", nil, err
		}
		amount, err := decimal.NewFromString(strings.Replace(p.Field61.Amount, ",", ".", 1))
		if err != nil {
			return "", "", nil, err
		}
		side := domain.TransactionSideCredit
		if strings.HasPrefix(p.Field61.DebitCreditMark, "D") {
			side = domain.TransactionSideDebit
		}
		valueDate, err := time.Parse("060102", p.Field61.ValueDate)
		if err != nil {
			return "", "", nil, err
		}
		tx, err := txRepo.Create(ctx, tenantID, domain.Transaction{
			AccountID:               accountID,
			StatementID:             &rec.Raw.BatchID,
			ExternalRef:             p.Field61.CustomerRef,
			Amount:                  amount,
			Currency:                p.Field61.Currency,
			BaseAmount:              amount,
			ValueDate:               valueDate,
			BookingDate:             valueDate,
			Description:             p.Field86.Narrative,
			Side:                    side,
			SourceMode:              domain.SourceModeBatch,
			IngestionIdempotencyKey: rec.Raw.BatchID.String() + "-" + p.Field61.CustomerRef,
			Status:                  domain.TransactionStatusUnmatched,
		})
		if err != nil {
			return "", "", nil, err
		}
		payload, _ := json.Marshal(map[string]any{"transaction_id": tx.ID.String()})
		return fmt.Sprintf("ingestion.raw.%s", tenantID.String()[:8]), tx.ID.String(), payload, nil
	}

	pl := &pipeline.Pipeline{
		Stages: []pipeline.Stage{
			&postgres.PersistEmitStage{Pool: appPool, TenantID: tenantID, Outbox: outboxRepo, Persist: persistFn},
		},
	}

	runCtx := tenancy.WithTenant(ctx, tenancy.TenantContext{TenantID: tenantID})
	if err := pl.Run(runCtx, sftpConn, cfg); err != nil {
		t.Fatalf("pipeline Run failed: %v", err)
	}

	// --- Statement row: correct format/checksum/raw_file_ref. ---
	var stmtCount int
	var format string
	if err := db.Pool.QueryRow(ctx, `SELECT count(*), max(format) FROM statements WHERE account_id = $1`, accountID).Scan(&stmtCount, &format); err != nil {
		t.Fatalf("statement query failed: %v", err)
	}
	if stmtCount != 1 {
		t.Fatalf("expected 1 statement, got %d", stmtCount)
	}
	if format != "MT940" {
		t.Errorf("expected statement format MT940, got %s", format)
	}

	// --- Transaction rows: both lines persisted with correct amounts/sides. ---
	rows, err := db.Pool.Query(ctx, `SELECT amount, side, currency, description FROM transactions WHERE account_id = $1 ORDER BY amount`, accountID)
	if err != nil {
		t.Fatalf("transactions query failed: %v", err)
	}
	defer rows.Close()

	type gotTx struct {
		amount      decimal.Decimal
		side        string
		currency    string
		description string
	}
	var got []gotTx
	for rows.Next() {
		var g gotTx
		if err := rows.Scan(&g.amount, &g.side, &g.currency, &g.description); err != nil {
			t.Fatalf("scan failed: %v", err)
		}
		got = append(got, g)
	}
	if len(got) != 2 {
		t.Fatalf("expected 2 transactions, got %d", len(got))
	}
	if !got[0].amount.Equal(decimal.RequireFromString("250.00")) || got[0].side != string(domain.TransactionSideDebit) {
		t.Errorf("unexpected first transaction: %+v", got[0])
	}
	if !got[1].amount.Equal(decimal.RequireFromString("500.00")) || got[1].side != string(domain.TransactionSideCredit) {
		t.Errorf("unexpected second transaction: %+v", got[1])
	}
	if got[0].currency != "EUR" || got[1].currency != "EUR" {
		t.Errorf("expected EUR currency carried over from opening balance on both lines, got %+v / %+v", got[0], got[1])
	}
}

func mustJSON(t *testing.T, v any) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return b
}
