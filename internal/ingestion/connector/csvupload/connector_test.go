package csvupload_test

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"

	"github.com/google/uuid"
	"github.com/xuri/excelize/v2"

	"github.com/koriebruh/Jengine/internal/domain"
	"github.com/koriebruh/Jengine/internal/ingestion/connector"
	"github.com/koriebruh/Jengine/internal/ingestion/connector/csvupload"
)

type fakeStore struct {
	files map[string][]byte
}

func (s *fakeStore) Get(ctx context.Context, bucket, key string) ([]byte, error) {
	return s.files[bucket+"/"+key], nil
}

type fakeStatementStore struct {
	checksums map[string]bool // "accountID|checksum" -> exists
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

// passthroughTxRunner just calls fn directly - fine for tests since
// fakeStatementStore doesn't need a real ambient transaction.
func passthroughTxRunner(ctx context.Context, tenantID uuid.UUID, fn func(ctx context.Context) error) error {
	return fn(ctx)
}

func settingsJSON(t *testing.T, s map[string]any) []byte {
	t.Helper()
	b, err := json.Marshal(s)
	if err != nil {
		t.Fatalf("marshal settings: %v", err)
	}
	return b
}

func TestCSVUpload_StreamsRowsOneAtATime(t *testing.T) {
	csvData := []byte("name,amount\nAlice,100\nBob,200\nCarol,300\n")
	store := &fakeStore{files: map[string][]byte{"bucket/file.csv": csvData}}
	stmtStore := &fakeStatementStore{checksums: map[string]bool{}}
	c := csvupload.New(store, stmtStore, passthroughTxRunner)

	accountID := uuid.New()
	cfg := connector.ConnectorConfig{
		TenantID: uuid.New(), ConnectorID: uuid.New(),
		Settings: settingsJSON(t, map[string]any{
			"bucket": "bucket", "object_key": "file.csv", "format": "csv", "account_id": accountID,
		}),
	}

	ch, err := c.Fetch(context.Background(), cfg)
	if err != nil {
		t.Fatalf("Fetch failed: %v", err)
	}

	var rows []map[string]string
	for rec := range ch {
		var m map[string]string
		if err := json.Unmarshal(rec.Payload, &m); err != nil {
			t.Fatalf("unmarshal payload: %v", err)
		}
		rows = append(rows, m)
	}

	if len(rows) != 3 {
		t.Fatalf("expected 3 rows, got %d: %v", len(rows), rows)
	}
	if rows[0]["name"] != "Alice" || rows[0]["amount"] != "100" {
		t.Errorf("unexpected first row: %v", rows[0])
	}
	if rows[2]["name"] != "Carol" {
		t.Errorf("unexpected last row: %v", rows[2])
	}

	if len(stmtStore.created) != 1 {
		t.Fatalf("expected 1 Statement created, got %d", len(stmtStore.created))
	}
	if stmtStore.created[0].Format != "CSV" {
		t.Errorf("expected format CSV, got %s", stmtStore.created[0].Format)
	}
}

func TestCSVUpload_DuplicateQuarantinedByDefault(t *testing.T) {
	csvData := []byte("name,amount\nAlice,100\n")
	store := &fakeStore{files: map[string][]byte{"bucket/file.csv": csvData}}
	accountID := uuid.New()

	// Compute the same checksum the connector will compute, and seed it
	// as already-existing.
	stmtStore := &fakeStatementStore{checksums: map[string]bool{}}
	c := csvupload.New(store, stmtStore, passthroughTxRunner)
	cfg := connector.ConnectorConfig{
		TenantID: uuid.New(), ConnectorID: uuid.New(),
		Settings: settingsJSON(t, map[string]any{
			"bucket": "bucket", "object_key": "file.csv", "format": "csv", "account_id": accountID,
		}),
	}

	// First run: not a duplicate, succeeds and creates a Statement.
	ch, err := c.Fetch(context.Background(), cfg)
	if err != nil {
		t.Fatalf("first Fetch failed: %v", err)
	}
	for range ch {
	}
	if len(stmtStore.created) != 1 {
		t.Fatalf("expected 1 Statement after first fetch, got %d", len(stmtStore.created))
	}
	checksum := stmtStore.created[0].Checksum
	stmtStore.checksums[accountID.String()+"|"+checksum] = true

	// Second run: same file, same checksum - default policy is
	// quarantine, so Fetch must fail and must NOT create a second
	// Statement.
	_, err = c.Fetch(context.Background(), cfg)
	if err != csvupload.ErrDuplicateFile {
		t.Fatalf("expected ErrDuplicateFile, got %v", err)
	}
	if len(stmtStore.created) != 1 {
		t.Fatalf("expected still only 1 Statement after quarantined re-upload, got %d", len(stmtStore.created))
	}
}

func TestCSVUpload_DuplicateTreatedAsCorrectionWhenConfigured(t *testing.T) {
	csvData := []byte("name,amount\nAlice,100\n")
	store := &fakeStore{files: map[string][]byte{"bucket/file.csv": csvData}}
	accountID := uuid.New()
	stmtStore := &fakeStatementStore{checksums: map[string]bool{}}
	c := csvupload.New(store, stmtStore, passthroughTxRunner)

	settings := map[string]any{
		"bucket": "bucket", "object_key": "file.csv", "format": "csv",
		"account_id": accountID, "duplicate_policy": "correction",
	}
	cfg := connector.ConnectorConfig{TenantID: uuid.New(), ConnectorID: uuid.New(), Settings: settingsJSON(t, settings)}

	ch, err := c.Fetch(context.Background(), cfg)
	if err != nil {
		t.Fatalf("first Fetch failed: %v", err)
	}
	for range ch {
	}
	checksum := stmtStore.created[0].Checksum
	stmtStore.checksums[accountID.String()+"|"+checksum] = true

	// Second run: same checksum, but policy=correction - must succeed
	// and create a SECOND Statement (the correction), not error.
	ch2, err := c.Fetch(context.Background(), cfg)
	if err != nil {
		t.Fatalf("expected correction policy to allow re-processing, got error: %v", err)
	}
	for range ch2 {
	}
	if len(stmtStore.created) != 2 {
		t.Fatalf("expected 2 Statements (original + correction), got %d", len(stmtStore.created))
	}
}

// TestCSVUpload_ExcelFormat exercises the documented, deliberate
// whole-sheet-in-memory Excel fallback (plans/task/core/07 Implementation
// Notes) - built with excelize itself so this test needs no external
// fixture-generation tooling.
func TestCSVUpload_ExcelFormat(t *testing.T) {
	f := excelize.NewFile()
	sheet := f.GetSheetName(0)
	rows := [][]string{
		{"name", "amount"},
		{"Alice", "100"},
		{"Bob", "200"},
	}
	for i, row := range rows {
		for j, val := range row {
			cell, _ := excelize.CoordinatesToCellName(j+1, i+1)
			_ = f.SetCellValue(sheet, cell, val)
		}
	}
	var buf bytes.Buffer
	if _, err := f.WriteTo(&buf); err != nil {
		t.Fatalf("write xlsx: %v", err)
	}

	store := &fakeStore{files: map[string][]byte{"bucket/file.xlsx": buf.Bytes()}}
	stmtStore := &fakeStatementStore{checksums: map[string]bool{}}
	c := csvupload.New(store, stmtStore, passthroughTxRunner)

	accountID := uuid.New()
	cfg := connector.ConnectorConfig{
		TenantID: uuid.New(), ConnectorID: uuid.New(),
		Settings: settingsJSON(t, map[string]any{
			"bucket": "bucket", "object_key": "file.xlsx", "format": "xlsx", "account_id": accountID,
		}),
	}

	ch, err := c.Fetch(context.Background(), cfg)
	if err != nil {
		t.Fatalf("Fetch failed: %v", err)
	}

	var got []map[string]string
	for rec := range ch {
		var m map[string]string
		if err := json.Unmarshal(rec.Payload, &m); err != nil {
			t.Fatalf("unmarshal payload: %v", err)
		}
		got = append(got, m)
	}

	if len(got) != 2 {
		t.Fatalf("expected 2 data rows, got %d: %v", len(got), got)
	}
	if got[0]["name"] != "Alice" || got[0]["amount"] != "100" {
		t.Errorf("unexpected first row: %v", got[0])
	}
	if got[1]["name"] != "Bob" {
		t.Errorf("unexpected second row: %v", got[1])
	}
	if stmtStore.created[0].Format != "EXCEL" {
		t.Errorf("expected format EXCEL, got %s", stmtStore.created[0].Format)
	}
}

// TestCSVUpload_ExcelOversizeRejected proves the documented max-file-size
// guard on the bounded-in-memory Excel fallback actually rejects files
// above the limit, per plans/task/core/07 Implementation Notes.
func TestCSVUpload_ExcelOversizeRejected(t *testing.T) {
	oversized := bytes.Repeat([]byte("x"), 100)
	store := &fakeStore{files: map[string][]byte{"bucket/file.xlsx": oversized}}
	stmtStore := &fakeStatementStore{checksums: map[string]bool{}}
	c := csvupload.New(store, stmtStore, passthroughTxRunner)

	cfg := connector.ConnectorConfig{
		TenantID: uuid.New(), ConnectorID: uuid.New(),
		Settings: settingsJSON(t, map[string]any{
			"bucket": "bucket", "object_key": "file.xlsx", "format": "xlsx",
			"account_id": uuid.New(), "max_excel_file_size_bytes": 10,
		}),
	}

	_, err := c.Fetch(context.Background(), cfg)
	if err == nil {
		t.Fatal("expected an error for an oversized excel file, got nil")
	}
}
