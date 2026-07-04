package sftp_test

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/koriebruh/Jengine/internal/domain"
	"github.com/koriebruh/Jengine/internal/ingestion/connector"
	"github.com/koriebruh/Jengine/internal/ingestion/connector/sftp"
	"github.com/koriebruh/Jengine/internal/ingestion/parsers/mt940"
	"github.com/koriebruh/Jengine/internal/testutil"
)

type envSecret struct{ password string }

func (e envSecret) Resolve(ctx context.Context, vaultPathRef string) (string, error) {
	return e.password, nil
}

type fakeStatementStore struct {
	checksums map[string]bool
	created   []domain.Statement
}

func (s *fakeStatementStore) ExistsByChecksum(ctx context.Context, tenantID, accountID uuid.UUID, checksum string) (bool, error) {
	return s.checksums[accountID.String()+"|"+checksum], nil
}

func (s *fakeStatementStore) Create(ctx context.Context, tenantID uuid.UUID, st domain.Statement) (domain.Statement, error) {
	st.ID = uuid.New()
	st.TenantID = tenantID
	s.created = append(s.created, st)
	return st, nil
}

func passthroughTxRunner(ctx context.Context, tenantID uuid.UUID, fn func(ctx context.Context) error) error {
	return fn(ctx)
}

const sampleMT940 = `:20:STMT0001
:25:1234567890
:28C:1
:60F:C240101EUR10000,00
:61:2401020103D250,00NTRFNONREF123
:86:PAYMENT TO SUPPLIER ABC
:62F:C240102EUR9750,00
-
`

func drainRecords(t *testing.T, ch <-chan connector.RawRecord) []connector.RawRecord {
	t.Helper()
	var recs []connector.RawRecord
	for r := range ch {
		recs = append(recs, r)
	}
	return recs
}

func TestSFTPConnector_RoundTripWithCheckpoint(t *testing.T) {
	if testing.Short() {
		t.Skip("requires Docker - skipped under -short; run make test-integration")
	}

	hostDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(hostDir, "statement1.sta"), []byte(sampleMT940), 0o644); err != nil {
		t.Fatalf("write fixture file: %v", err)
	}

	srv := testutil.StartSFTP(t, hostDir)

	stmtStore := &fakeStatementStore{checksums: map[string]bool{}}
	// No real Postgres pool needed here since fakeStatementStore doesn't
	// require an ambient transaction - a real *pgxpool.Pool is only
	// needed by the production postgres.WithTx-backed TxRunner.
	conn := &sftp.Connector{
		Secrets:    envSecret{password: srv.Password},
		Statements: stmtStore,
	}
	conn.TxRunner = passthroughTxRunner

	accountID := uuid.New()
	tenantID := uuid.New()
	settings, _ := json.Marshal(map[string]any{
		"host": srv.Host, "username": srv.Username,
		"auth":         map[string]any{"type": "password", "vault_path_ref": "unused"},
		"remote_dir":   "/upload",
		"account_id":   accountID,
		"parse_format": "MT940",
		"dialect":      "generic",
	})
	cfg := connector.ConnectorConfig{TenantID: tenantID, ConnectorID: uuid.New(), Settings: settings}

	// --- First poll: statement1.sta is new, must be processed. ---
	ch, err := conn.Fetch(context.Background(), cfg)
	if err != nil {
		t.Fatalf("first Fetch failed: %v", err)
	}
	recs := drainRecords(t, ch)
	if len(recs) != 1 {
		t.Fatalf("expected 1 record (1 transaction line) from first poll, got %d", len(recs))
	}
	if len(stmtStore.created) != 1 {
		t.Fatalf("expected 1 Statement created on first poll, got %d", len(stmtStore.created))
	}

	cursor, err := conn.Checkpoint()
	if err != nil {
		t.Fatalf("Checkpoint failed: %v", err)
	}
	if len(cursor.State) == 0 {
		t.Fatal("expected non-empty cursor state after first poll")
	}

	// --- Second poll, same file unchanged: must NOT reprocess it. ---
	conn2 := &sftp.Connector{Secrets: envSecret{password: srv.Password}, Statements: stmtStore, InitialCursor: cursor}
	conn2.TxRunner = passthroughTxRunner

	ch2, err := conn2.Fetch(context.Background(), cfg)
	if err != nil {
		t.Fatalf("second Fetch failed: %v", err)
	}
	recs2 := drainRecords(t, ch2)
	if len(recs2) != 0 {
		t.Fatalf("expected 0 records on second poll (file unchanged), got %d", len(recs2))
	}
	if len(stmtStore.created) != 1 {
		t.Fatalf("expected still only 1 Statement after second poll (no reprocessing), got %d", len(stmtStore.created))
	}

	// --- Add a NEW file, poll again with the same cursor: only the new
	// file should be processed. ---
	if err := os.WriteFile(filepath.Join(hostDir, "statement2.sta"), []byte(sampleMT940), 0o644); err != nil {
		t.Fatalf("write second fixture file: %v", err)
	}
	// Give the bind mount a moment to reflect the new file inside the
	// container (should be immediate for a real bind mount, but a small
	// wait avoids flakiness on slower CI filesystems).
	time.Sleep(200 * time.Millisecond)

	cursor2, err := conn2.Checkpoint()
	if err != nil {
		t.Fatalf("Checkpoint after second poll failed: %v", err)
	}
	conn3 := &sftp.Connector{Secrets: envSecret{password: srv.Password}, Statements: stmtStore, InitialCursor: cursor2}
	conn3.TxRunner = passthroughTxRunner

	ch3, err := conn3.Fetch(context.Background(), cfg)
	if err != nil {
		t.Fatalf("third Fetch failed: %v", err)
	}
	recs3 := drainRecords(t, ch3)
	if len(recs3) != 1 {
		t.Fatalf("expected 1 record from the newly added file, got %d", len(recs3))
	}
	if len(stmtStore.created) != 2 {
		t.Fatalf("expected 2 Statements total after the new file, got %d", len(stmtStore.created))
	}
}

// TestSFTPConnector_DuplicateFileQuarantinedByDefault proves the same
// checksum-based duplicate policy csvupload has also holds for the SFTP
// connector: a file whose checksum matches a Statement already recorded
// for the account is quarantined (the mtime is still recorded so it
// isn't retried every poll, but no Transaction rows are emitted and no
// second Statement is created) under the default "quarantine" policy.
func TestSFTPConnector_DuplicateFileQuarantinedByDefault(t *testing.T) {
	if testing.Short() {
		t.Skip("requires Docker - skipped under -short; run make test-integration")
	}

	hostDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(hostDir, "statement1.sta"), []byte(sampleMT940), 0o644); err != nil {
		t.Fatalf("write fixture file: %v", err)
	}
	srv := testutil.StartSFTP(t, hostDir)

	accountID := uuid.New()
	stmtStore := &fakeStatementStore{checksums: map[string]bool{}}
	settings, _ := json.Marshal(map[string]any{
		"host": srv.Host, "username": srv.Username,
		"auth":         map[string]any{"type": "password", "vault_path_ref": "unused"},
		"remote_dir":   "/upload",
		"account_id":   accountID,
		"parse_format": "MT940",
	})
	cfg := connector.ConnectorConfig{TenantID: uuid.New(), ConnectorID: uuid.New(), Settings: settings}

	// First poll: not a duplicate, processes normally.
	conn := &sftp.Connector{Secrets: envSecret{password: srv.Password}, Statements: stmtStore}
	conn.TxRunner = passthroughTxRunner
	ch, err := conn.Fetch(context.Background(), cfg)
	if err != nil {
		t.Fatalf("first Fetch failed: %v", err)
	}
	drainRecords(t, ch)
	if len(stmtStore.created) != 1 {
		t.Fatalf("expected 1 Statement after first poll, got %d", len(stmtStore.created))
	}
	checksum := stmtStore.created[0].Checksum
	stmtStore.checksums[accountID.String()+"|"+checksum] = true

	// Force a re-poll of the SAME unchanged file by clearing the cursor
	// (simulating, e.g., a cursor being lost/reset) - the checksum-based
	// duplicate check must still catch it independent of the mtime-based
	// cursor skip.
	conn2 := &sftp.Connector{Secrets: envSecret{password: srv.Password}, Statements: stmtStore}
	conn2.TxRunner = passthroughTxRunner
	ch2, err := conn2.Fetch(context.Background(), cfg)
	if err != nil {
		t.Fatalf("second Fetch failed: %v", err)
	}
	recs2 := drainRecords(t, ch2)
	if len(recs2) != 0 {
		t.Fatalf("expected 0 records for the quarantined duplicate, got %d", len(recs2))
	}
	if len(stmtStore.created) != 1 {
		t.Fatalf("expected still only 1 Statement after the quarantined duplicate re-poll, got %d", len(stmtStore.created))
	}
}

func TestSFTPConnector_ParsesFieldNamesForTask08Contract(t *testing.T) {
	if testing.Short() {
		t.Skip("requires Docker - skipped under -short; run make test-integration")
	}

	hostDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(hostDir, "statement1.sta"), []byte(sampleMT940), 0o644); err != nil {
		t.Fatalf("write fixture file: %v", err)
	}
	srv := testutil.StartSFTP(t, hostDir)

	stmtStore := &fakeStatementStore{checksums: map[string]bool{}}
	conn := &sftp.Connector{Secrets: envSecret{password: srv.Password}, Statements: stmtStore}
	conn.TxRunner = passthroughTxRunner

	accountID := uuid.New()
	settings, _ := json.Marshal(map[string]any{
		"host": srv.Host, "username": srv.Username,
		"auth":         map[string]any{"type": "password", "vault_path_ref": "unused"},
		"remote_dir":   "/upload",
		"account_id":   accountID,
		"parse_format": "MT940",
	})
	cfg := connector.ConnectorConfig{TenantID: uuid.New(), ConnectorID: uuid.New(), Settings: settings}

	ch, err := conn.Fetch(context.Background(), cfg)
	if err != nil {
		t.Fatalf("Fetch failed: %v", err)
	}
	recs := drainRecords(t, ch)
	if len(recs) != 1 {
		t.Fatalf("expected 1 record, got %d", len(recs))
	}

	var payload struct {
		Field61 mt940.Field61 `json:"field_61"`
		Field86 mt940.Field86 `json:"field_86"`
	}
	if err := json.Unmarshal(recs[0].Payload, &payload); err != nil {
		t.Fatalf("unmarshal record payload: %v", err)
	}
	if payload.Field61.Amount != "250,00" || payload.Field61.DebitCreditMark != "D" || payload.Field61.Currency != "EUR" {
		t.Errorf("unexpected field_61: %+v", payload.Field61)
	}
	if payload.Field86.Narrative != "PAYMENT TO SUPPLIER ABC" {
		t.Errorf("unexpected field_86: %+v", payload.Field86)
	}
}
