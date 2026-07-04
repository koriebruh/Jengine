// Package sftp implements the SFTP poller SourceConnector
// (plans/task/core/07): connects over SSH/SFTP, lists a remote
// directory, skips files already processed (tracked via
// connector.Cursor), optionally PGP-decrypts, and hands the resulting
// bytes to a format parser (MT940 at MVP).
package sftp

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"path"
	"strings"
	"time"

	"github.com/ProtonMail/go-crypto/openpgp"
	"github.com/google/uuid"
	pkgsftp "github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"

	"github.com/koriebruh/Jengine/internal/domain"
	"github.com/koriebruh/Jengine/internal/ingestion/connector"
	"github.com/koriebruh/Jengine/internal/ingestion/parsers/mt940"
)

// SecretResolver resolves a Vault path reference to its secret value.
// Real Vault integration is plans/task/core/23 - until then, callers
// inject whatever resolver fits local dev/testing (e.g. an env-var-backed
// one), but the interface seam itself keeps "never inline secrets" true
// from day one (plans/docs/16-development-workflow.md §16.3): nothing in
// ConnectorConfig.Settings ever holds a raw password/key, only a
// vault_path_ref string this interface resolves at use-time.
type SecretResolver interface {
	Resolve(ctx context.Context, vaultPathRef string) (string, error)
}

// StatementStore is the surface this connector needs for duplicate-file
// detection and Statement row creation - the same shape
// internal/ingestion/connector/csvupload.StatementStore uses,
// internal/storage/postgres.StatementRepo satisfies both structurally.
type StatementStore interface {
	ExistsByChecksum(ctx context.Context, tenantID, accountID uuid.UUID, checksum string) (bool, error)
	Create(ctx context.Context, tenantID uuid.UUID, s domain.Statement) (domain.Statement, error)
}

type authSettings struct {
	Type         string `json:"type"` // "password" | "key"
	VaultPathRef string `json:"vault_path_ref"`
}

type pgpSettings struct {
	Enabled      bool   `json:"enabled"`
	VaultPathRef string `json:"vault_path_ref"` // private key, armored
}

type settings struct {
	Host            string                      `json:"host"` // "host:port"
	Username        string                      `json:"username"`
	Auth            authSettings                `json:"auth"`
	RemoteDir       string                      `json:"remote_dir"`
	AccountID       uuid.UUID                   `json:"account_id"`
	ParseFormat     string                      `json:"parse_format"` // "MT940"
	Dialect         string                      `json:"dialect"`
	PGP             pgpSettings                 `json:"pgp"`
	DuplicatePolicy csvuploadDuplicatePolicyRef `json:"duplicate_policy"`
}

// csvuploadDuplicatePolicyRef avoids an import cycle/dependency on the
// csvupload package for just this one string type - same two values
// ("quarantine"/"correction"), same meaning.
type csvuploadDuplicatePolicyRef string

const (
	duplicatePolicyQuarantine csvuploadDuplicatePolicyRef = "quarantine"
	duplicatePolicyCorrection csvuploadDuplicatePolicyRef = "correction"
)

// ErrDuplicateFile mirrors csvupload.ErrDuplicateFile for this connector's
// own quarantine policy branch.
var ErrDuplicateFile = fmt.Errorf("sftp: duplicate file checksum for this account")

// cursorState is the opaque shape stored in connector.Cursor.State -
// maps remote filename to the mtime (Unix seconds) it had when last
// processed, so a re-poll of an unchanged directory doesn't reprocess
// files whose content hasn't changed.
type cursorState struct {
	ProcessedFiles map[string]int64 `json:"processed_files"`
}

// TxRunner wraps fn in a transaction scoped to tenantID - same shape and
// rationale as internal/ingestion/connector/csvupload.TxRunner: Fetch
// (stage 1, Raw Fetch) is called directly by pipeline.Pipeline.Run,
// outside any ambient transaction, but this connector's Fetch must itself
// write a Statement row, so it needs to open its own transaction rather
// than assume the caller already provided one. Satisfied by a thin
// closure around postgres.WithTx in production; tests can pass a
// pass-through that just calls fn directly.
type TxRunner func(ctx context.Context, tenantID uuid.UUID, fn func(ctx context.Context) error) error

// Connector implements connector.SourceConnector for SFTP polling.
type Connector struct {
	Secrets    SecretResolver
	Statements StatementStore
	TxRunner   TxRunner

	// InitialCursor should be set (from the connector's persisted
	// CursorState, loaded via domain.ConnectorRepository) before each
	// Fetch call - the SourceConnector interface's fixed signature
	// (plans/task/core/06) has no room to pass this through
	// ConnectorConfig, so the caller (the cron dispatcher in
	// cmd/ingestion-gateway) is responsible for wiring it in.
	InitialCursor connector.Cursor

	lastCursor connector.Cursor
}

func New(txRunner TxRunner, secrets SecretResolver, statements StatementStore) *Connector {
	return &Connector{TxRunner: txRunner, Secrets: secrets, Statements: statements}
}

func (c *Connector) SupportsStreaming() bool { return false }

func (c *Connector) Checkpoint() (connector.Cursor, error) {
	return c.lastCursor, nil
}

func (c *Connector) Validate(cfg connector.ConnectorConfig) error {
	_, err := parseSettings(cfg.Settings)
	return err
}

func parseSettings(raw []byte) (settings, error) {
	var s settings
	if err := json.Unmarshal(raw, &s); err != nil {
		return s, fmt.Errorf("sftp: invalid settings: %w", err)
	}
	if s.Host == "" || s.Username == "" || s.RemoteDir == "" {
		return s, fmt.Errorf("sftp: settings.host, username, and remote_dir are required")
	}
	if s.Auth.Type != "password" && s.Auth.Type != "key" {
		return s, fmt.Errorf("sftp: settings.auth.type must be \"password\" or \"key\", got %q", s.Auth.Type)
	}
	if s.AccountID == uuid.Nil {
		return s, fmt.Errorf("sftp: settings.account_id is required")
	}
	if s.DuplicatePolicy == "" {
		s.DuplicatePolicy = duplicatePolicyQuarantine
	}
	return s, nil
}

func (c *Connector) dial(ctx context.Context, s settings) (*pkgsftp.Client, func(), error) {
	secret, err := c.Secrets.Resolve(ctx, s.Auth.VaultPathRef)
	if err != nil {
		return nil, nil, fmt.Errorf("sftp: resolve credentials: %w", err)
	}

	var authMethod ssh.AuthMethod
	switch s.Auth.Type {
	case "password":
		authMethod = ssh.Password(secret)
	case "key":
		signer, err := ssh.ParsePrivateKey([]byte(secret))
		if err != nil {
			return nil, nil, fmt.Errorf("sftp: parse private key: %w", err)
		}
		authMethod = ssh.PublicKeys(signer)
	}

	sshConfig := &ssh.ClientConfig{
		User:            s.Username,
		Auth:            []ssh.AuthMethod{authMethod},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(), //nolint:gosec // MVP local dev; host-key pinning is a hardening follow-up (plans/task/core/23)
		Timeout:         15 * time.Second,
	}

	sshClient, err := ssh.Dial("tcp", s.Host, sshConfig)
	if err != nil {
		return nil, nil, fmt.Errorf("sftp: ssh dial: %w", err)
	}

	sftpClient, err := pkgsftp.NewClient(sshClient)
	if err != nil {
		_ = sshClient.Close()
		return nil, nil, fmt.Errorf("sftp: new sftp client: %w", err)
	}

	cleanup := func() {
		_ = sftpClient.Close()
		_ = sshClient.Close()
	}
	return sftpClient, cleanup, nil
}

// Fetch lists RemoteDir, skips files whose mtime matches the prior
// cursor, and streams RawRecords for every new/changed file's parsed
// content.
func (c *Connector) Fetch(ctx context.Context, cfg connector.ConnectorConfig) (<-chan connector.RawRecord, error) {
	s, err := parseSettings(cfg.Settings)
	if err != nil {
		return nil, err
	}

	prior := cursorState{ProcessedFiles: map[string]int64{}}
	if len(c.InitialCursor.State) > 0 {
		if err := json.Unmarshal(c.InitialCursor.State, &prior); err != nil {
			return nil, fmt.Errorf("sftp: invalid prior cursor state: %w", err)
		}
		if prior.ProcessedFiles == nil {
			prior.ProcessedFiles = map[string]int64{}
		}
	}

	client, cleanup, err := c.dial(ctx, s)
	if err != nil {
		return nil, err
	}

	entries, err := client.ReadDir(s.RemoteDir)
	if err != nil {
		cleanup()
		return nil, fmt.Errorf("sftp: list %s: %w", s.RemoteDir, err)
	}

	next := cursorState{ProcessedFiles: map[string]int64{}}
	for k, v := range prior.ProcessedFiles {
		next.ProcessedFiles[k] = v
	}

	type pendingFile struct {
		name        string
		statementID uuid.UUID
		lines       []mt940.TransactionLine
	}
	var toEmit []pendingFile

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		mtime := entry.ModTime().Unix()
		if prev, ok := prior.ProcessedFiles[name]; ok && prev == mtime {
			continue // unchanged since last poll
		}

		remotePath := path.Join(s.RemoteDir, name)
		data, err := readRemoteFile(client, remotePath)
		if err != nil {
			cleanup()
			return nil, fmt.Errorf("sftp: read %s: %w", remotePath, err)
		}

		if s.PGP.Enabled {
			data, err = decryptPGP(ctx, c.Secrets, s.PGP.VaultPathRef, data)
			if err != nil {
				cleanup()
				return nil, fmt.Errorf("sftp: pgp decrypt %s: %w", remotePath, err)
			}
		}

		sum := sha256.Sum256(data)
		checksum := hex.EncodeToString(sum[:])

		var stmt mt940.Statement
		switch strings.ToUpper(s.ParseFormat) {
		case "MT940", "":
			stmt, err = mt940.Parse(data, s.Dialect)
		default:
			err = fmt.Errorf("unsupported parse_format %q", s.ParseFormat)
		}
		if err != nil {
			cleanup()
			return nil, fmt.Errorf("sftp: parse %s: %w", name, err)
		}

		var statementID uuid.UUID
		var quarantinedDuplicate bool
		err = c.TxRunner(ctx, cfg.TenantID, func(ctx context.Context) error {
			exists, err := c.Statements.ExistsByChecksum(ctx, cfg.TenantID, s.AccountID, checksum)
			if err != nil {
				return fmt.Errorf("duplicate check: %w", err)
			}
			if exists && s.DuplicatePolicy == duplicatePolicyQuarantine {
				quarantinedDuplicate = true
				return nil
			}

			statement, err := c.Statements.Create(ctx, cfg.TenantID, domain.Statement{
				AccountID:         s.AccountID,
				SourceConnectorID: &cfg.ConnectorID,
				Format:            "MT940",
				ReceivedAt:        time.Now(),
				Status:            domain.StatementStatusReceived,
				RawFileRef:        remotePath,
				Checksum:          checksum,
			})
			if err != nil {
				return fmt.Errorf("create statement: %w", err)
			}
			statementID = statement.ID
			return nil
		})
		if err != nil {
			cleanup()
			return nil, fmt.Errorf("sftp: %s: %w", name, err)
		}

		next.ProcessedFiles[name] = mtime
		if quarantinedDuplicate {
			// This file is skipped (quarantined), but its mtime is still
			// recorded so it isn't retried every poll - the failure is
			// visible via the file not producing any Transaction rows,
			// which the caller can reconcile against directory listing.
			continue
		}
		toEmit = append(toEmit, pendingFile{name: name, statementID: statementID, lines: stmt.Lines})
	}

	nextState, err := json.Marshal(next)
	if err != nil {
		cleanup()
		return nil, fmt.Errorf("sftp: marshal cursor state: %w", err)
	}
	c.lastCursor = connector.Cursor{ConnectorID: cfg.ConnectorID, State: nextState, UpdatedAt: time.Now()}

	ch := make(chan connector.RawRecord)
	go func() {
		defer cleanup()
		defer close(ch)
		for _, f := range toEmit {
			for _, line := range f.lines {
				payload, err := json.Marshal(map[string]any{"field_61": line.Field61, "field_86": line.Field86})
				if err != nil {
					continue
				}
				rec := connector.RawRecord{
					TenantID: cfg.TenantID, ConnectorID: cfg.ConnectorID,
					SourceFormat: "MT940", Payload: payload, ReceivedAt: time.Now(),
					BatchID: f.statementID, SourceMode: domain.SourceModeBatch,
				}
				select {
				case ch <- rec:
				case <-ctx.Done():
					return
				}
			}
		}
	}()
	return ch, nil
}

func readRemoteFile(client *pkgsftp.Client, remotePath string) ([]byte, error) {
	f, err := client.Open(remotePath)
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()
	return io.ReadAll(f)
}

// decryptPGP decrypts data with the private key resolved from
// vaultPathRef. Skipped entirely by the caller when PGP is disabled -
// plain-file mode is the default and must work standalone
// (plans/task/core/07 Implementation Notes).
func decryptPGP(ctx context.Context, secrets SecretResolver, vaultPathRef string, data []byte) ([]byte, error) {
	armoredKey, err := secrets.Resolve(ctx, vaultPathRef)
	if err != nil {
		return nil, fmt.Errorf("resolve pgp key: %w", err)
	}

	keyring, err := openpgp.ReadArmoredKeyRing(strings.NewReader(armoredKey))
	if err != nil {
		return nil, fmt.Errorf("read pgp key ring: %w", err)
	}

	md, err := openpgp.ReadMessage(bytes.NewReader(data), keyring, nil, nil)
	if err != nil {
		return nil, fmt.Errorf("read pgp message: %w", err)
	}

	plaintext, err := io.ReadAll(md.UnverifiedBody)
	if err != nil {
		return nil, fmt.Errorf("read decrypted body: %w", err)
	}
	return plaintext, nil
}

var _ connector.SourceConnector = (*Connector)(nil)
