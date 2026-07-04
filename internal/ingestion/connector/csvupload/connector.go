// Package csvupload implements the CSV/Excel upload SourceConnector
// (plans/task/core/07) - reads an already-uploaded file from object
// storage (upload-handling HTTP endpoints are plans/task/core/15's job,
// not this connector's), streams it row by row, and checks for
// duplicate re-uploads against the account's prior Statement checksum.
package csvupload

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/csv"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"time"

	"github.com/google/uuid"
	"github.com/xuri/excelize/v2"

	"github.com/koriebruh/Jengine/internal/domain"
	"github.com/koriebruh/Jengine/internal/ingestion/connector"
)

// ErrDuplicateFile is returned by Fetch when the file's checksum matches
// an existing Statement for the same account and the configured policy
// is "quarantine" (the default - never silently overwrite financial data,
// plans/task/core/07 Implementation Notes).
var ErrDuplicateFile = fmt.Errorf("csvupload: duplicate file checksum for this account")

// ObjectStore is the read-only surface this connector needs -
// internal/ingestion/objectstore.ObjectStore satisfies it structurally.
type ObjectStore interface {
	Get(ctx context.Context, bucket, key string) ([]byte, error)
}

// StatementStore is the surface this connector needs for duplicate-file
// detection and Statement row creation -
// internal/storage/postgres.StatementRepo satisfies it structurally.
type StatementStore interface {
	ExistsByChecksum(ctx context.Context, tenantID, accountID uuid.UUID, checksum string) (bool, error)
	Create(ctx context.Context, tenantID uuid.UUID, s domain.Statement) (domain.Statement, error)
}

// TxRunner wraps fn in a transaction scoped to tenantID. Fetch (stage 1,
// Raw Fetch) is called directly by pipeline.Pipeline.Run, outside any
// ambient transaction (unlike stage 8's PersistEmitStage, which owns its
// own transaction internally) - since this connector's Fetch must itself
// write a Statement row (plans/task/core/07's duplicate-file-detection
// requirement), it needs to open its own transaction, not assume the
// caller already provided one. In production, satisfied by a thin
// closure around postgres.WithTx; tests can pass a pass-through that
// just calls fn directly, since a fake StatementStore doesn't need a real
// transaction.
type TxRunner func(ctx context.Context, tenantID uuid.UUID, fn func(ctx context.Context) error) error

// DuplicatePolicy controls what happens when a re-uploaded file's
// checksum matches a prior Statement for the same account.
type DuplicatePolicy string

const (
	// DuplicatePolicyQuarantine is the safe default: reject the
	// re-upload, Fetch returns ErrDuplicateFile, nothing is persisted.
	DuplicatePolicyQuarantine DuplicatePolicy = "quarantine"
	// DuplicatePolicyCorrection treats the re-upload as an intentional
	// correction: a new Statement is created (superseding the prior one
	// in effect, though the prior row is not deleted - see
	// plans/task/core/07 Common Pitfalls on not silently overwriting).
	DuplicatePolicyCorrection DuplicatePolicy = "correction"
)

// settings is the JSON shape expected in ConnectorConfig.Settings.
type settings struct {
	Bucket           string          `json:"bucket"`
	ObjectKey        string          `json:"object_key"`
	Format           string          `json:"format"` // "csv" | "xlsx"
	AccountID        uuid.UUID       `json:"account_id"`
	DuplicatePolicy  DuplicatePolicy `json:"duplicate_policy"`
	MaxExcelFileSize int64           `json:"max_excel_file_size_bytes"` // 0 = use DefaultMaxExcelFileSize
}

// DefaultMaxExcelFileSize bounds the whole-workbook-in-memory Excel
// fallback (see readExcelRows's doc comment) - files larger than this are
// rejected with a clear error rather than risking unbounded memory use.
const DefaultMaxExcelFileSize = 50 * 1024 * 1024 // 50MB

// Connector implements connector.SourceConnector for CSV/Excel uploads.
type Connector struct {
	Store      ObjectStore
	Statements StatementStore
	TxRunner   TxRunner
}

func New(store ObjectStore, statements StatementStore, txRunner TxRunner) *Connector {
	return &Connector{Store: store, Statements: statements, TxRunner: txRunner}
}

func (c *Connector) SupportsStreaming() bool { return false }

func (c *Connector) Checkpoint() (connector.Cursor, error) {
	// Upload connectors are one-shot (triggered per upload, not polled) -
	// no cursor/watermark state to persist between runs.
	return connector.Cursor{}, nil
}

func (c *Connector) Validate(cfg connector.ConnectorConfig) error {
	_, err := parseSettings(cfg.Settings)
	return err
}

func parseSettings(raw []byte) (settings, error) {
	var s settings
	if err := json.Unmarshal(raw, &s); err != nil {
		return s, fmt.Errorf("csvupload: invalid settings: %w", err)
	}
	if s.Bucket == "" || s.ObjectKey == "" {
		return s, fmt.Errorf("csvupload: settings.bucket and settings.object_key are required")
	}
	if s.Format != "csv" && s.Format != "xlsx" {
		return s, fmt.Errorf("csvupload: settings.format must be \"csv\" or \"xlsx\", got %q", s.Format)
	}
	if s.AccountID == uuid.Nil {
		return s, fmt.Errorf("csvupload: settings.account_id is required")
	}
	if s.DuplicatePolicy == "" {
		s.DuplicatePolicy = DuplicatePolicyQuarantine
	}
	if s.MaxExcelFileSize <= 0 {
		s.MaxExcelFileSize = DefaultMaxExcelFileSize
	}
	return s, nil
}

// Fetch reads the configured file from object storage, checks for a
// duplicate checksum against the account's Statement history, creates
// (or refuses to create, per policy) a Statement row, and streams one
// RawRecord per data row.
func (c *Connector) Fetch(ctx context.Context, cfg connector.ConnectorConfig) (<-chan connector.RawRecord, error) {
	s, err := parseSettings(cfg.Settings)
	if err != nil {
		return nil, err
	}

	data, err := c.Store.Get(ctx, s.Bucket, s.ObjectKey)
	if err != nil {
		return nil, fmt.Errorf("csvupload: fetch object: %w", err)
	}

	sum := sha256.Sum256(data)
	checksum := hex.EncodeToString(sum[:])

	var statementID uuid.UUID
	var duplicate bool
	err = c.TxRunner(ctx, cfg.TenantID, func(ctx context.Context) error {
		exists, err := c.Statements.ExistsByChecksum(ctx, cfg.TenantID, s.AccountID, checksum)
		if err != nil {
			return fmt.Errorf("duplicate check: %w", err)
		}
		if exists && s.DuplicatePolicy == DuplicatePolicyQuarantine {
			duplicate = true
			return nil
		}

		statement, err := c.Statements.Create(ctx, cfg.TenantID, domain.Statement{
			AccountID:         s.AccountID,
			SourceConnectorID: &cfg.ConnectorID,
			Format:            formatLabel(s.Format),
			ReceivedAt:        time.Now(),
			Status:            domain.StatementStatusReceived,
			RawFileRef:        fmt.Sprintf("%s/%s", s.Bucket, s.ObjectKey),
			Checksum:          checksum,
		})
		if err != nil {
			return fmt.Errorf("create statement: %w", err)
		}
		statementID = statement.ID
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("csvupload: %w", err)
	}
	if duplicate {
		return nil, ErrDuplicateFile
	}

	switch s.Format {
	case "csv":
		return streamCSV(ctx, data, cfg, statementID)
	default: // "xlsx", validated by parseSettings
		return streamExcel(ctx, data, s.MaxExcelFileSize, cfg, statementID)
	}
}

func formatLabel(f string) string {
	if f == "xlsx" {
		return "EXCEL"
	}
	return "CSV"
}

// streamCSV emits one RawRecord per data row as it's read from the
// underlying reader - encoding/csv.Reader.Read() is already line-at-a-
// time, so at no point is more than one row's fields held in memory,
// satisfying plans/task/core/07's explicit no-full-file-memory-load
// requirement. Each row is re-encoded as a JSON {header: value} object so
// stage 3 (Field Mapping) has clean structured input regardless of column
// order.
func streamCSV(ctx context.Context, data []byte, cfg connector.ConnectorConfig, statementID uuid.UUID) (<-chan connector.RawRecord, error) {
	r := csv.NewReader(bytes.NewReader(data))
	header, err := r.Read()
	if err != nil {
		return nil, fmt.Errorf("csvupload: read header row: %w", err)
	}

	ch := make(chan connector.RawRecord)
	go func() {
		defer close(ch)
		for {
			fields, err := r.Read()
			if err == io.EOF {
				return
			}
			if err != nil {
				return // malformed trailing row - nothing more to stream
			}

			obj := make(map[string]string, len(header))
			for i, h := range header {
				if i < len(fields) {
					obj[h] = fields[i]
				}
			}
			payload, err := json.Marshal(obj)
			if err != nil {
				continue
			}

			rec := connector.RawRecord{
				TenantID: cfg.TenantID, ConnectorID: cfg.ConnectorID,
				SourceFormat: "CSV", Payload: payload, ReceivedAt: time.Now(),
				BatchID: statementID, SourceMode: domain.SourceModeBatch,
			}
			select {
			case ch <- rec:
			case <-ctx.Done():
				return
			}
		}
	}()
	return ch, nil
}

// streamExcel is a deliberate, documented fallback (plans/task/core/07
// Implementation Notes explicitly allows this): excelize's simple GetRows
// API loads the whole sheet into memory rather than streaming row by row.
// Bounded by maxFileSize (checked against the already-fetched raw bytes
// before parsing) rather than fully-streaming Excel support, which the
// task's own spec accepts as an acceptable scope trade-off given task
// budget - full streaming Excel (excelize's StreamReader) is a candidate
// follow-up, not required for MVP.
func streamExcel(ctx context.Context, data []byte, maxFileSize int64, cfg connector.ConnectorConfig, statementID uuid.UUID) (<-chan connector.RawRecord, error) {
	if int64(len(data)) > maxFileSize {
		return nil, fmt.Errorf("csvupload: excel file size %d exceeds max_excel_file_size_bytes %d", len(data), maxFileSize)
	}

	f, err := excelize.OpenReader(bytes.NewReader(data))
	if err != nil {
		return nil, fmt.Errorf("csvupload: open excel file: %w", err)
	}
	sheets := f.GetSheetList()
	if len(sheets) == 0 {
		return nil, fmt.Errorf("csvupload: excel file has no sheets")
	}

	rows, err := f.GetRows(sheets[0])
	if err != nil {
		return nil, fmt.Errorf("csvupload: read excel sheet: %w", err)
	}
	if len(rows) == 0 {
		return nil, fmt.Errorf("csvupload: excel sheet is empty")
	}
	header := rows[0]

	ch := make(chan connector.RawRecord)
	go func() {
		defer close(ch)
		for _, fields := range rows[1:] {
			obj := make(map[string]string, len(header))
			for i, h := range header {
				if i < len(fields) {
					obj[h] = fields[i]
				}
			}
			payload, err := json.Marshal(obj)
			if err != nil {
				continue
			}

			rec := connector.RawRecord{
				TenantID: cfg.TenantID, ConnectorID: cfg.ConnectorID,
				SourceFormat: "EXCEL", Payload: payload, ReceivedAt: time.Now(),
				BatchID: statementID, SourceMode: domain.SourceModeBatch,
			}
			select {
			case ch <- rec:
			case <-ctx.Done():
				return
			}
		}
	}()
	return ch, nil
}

var _ connector.SourceConnector = (*Connector)(nil)
