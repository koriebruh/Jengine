// Package webhookreceiver implements the inbound webhook-receiver
// SourceConnector (plans/task/core/18): a payment gateway (Stripe/Adyen-
// style) pushes signed settlement events to Jengine over HTTPS. This is
// the INBOUND direction only - do not confuse with plans/task/core/21's
// webhook system, which is OUTBOUND notifications Jengine sends to
// tenants; the two share nothing but the word "webhook".
//
// Unlike this repo's other connectors (SFTP/CSV, which pull), this one
// is push-based: ServeHTTP is mounted at
// /v1/webhooks/ingest/{tenant_id}/{connector_id} by whichever cmd/*
// wires it up, and Fetch just exposes the shared channel ServeHTTP
// enqueues onto - there's no polling loop here.
package webhookreceiver

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/koriebruh/Jengine/internal/domain"
	"github.com/koriebruh/Jengine/internal/ingestion/connector"
)

// maxBodyBytes bounds the request body read - an untrusted inbound
// connector must never let a caller drive unbounded memory growth via
// Content-Length lying or chunked transfer.
const maxBodyBytes = 5 << 20 // 5 MiB

// SecretResolver resolves a Vault path reference to its secret value -
// same shape as connector/sftp.SecretResolver; each connector package
// keeps its own local copy rather than sharing one type, for package
// independence (established convention, see sftp.SecretResolver's own
// doc comment).
type SecretResolver interface {
	Resolve(ctx context.Context, vaultPathRef string) (string, error)
}

// Config is this connector's ConnectorConfig.Settings shape.
type Config struct {
	HMACSecretRef    string `json:"hmac_secret_ref"`
	SignatureHeader  string `json:"signature_header"`
	SignatureScheme  string `json:"signature_scheme"` // "stripe" | "adyen" | "generic-hmac-sha256"
	DeliveryIDHeader string `json:"delivery_id_header,omitempty"`
	SourceFormat     string `json:"source_format"` // field-mapping DSL's source_format key for this provider's JSON shape
}

func (c Config) validate() error {
	if c.HMACSecretRef == "" {
		return fmt.Errorf("webhookreceiver: hmac_secret_ref is required")
	}
	if c.SignatureHeader == "" {
		return fmt.Errorf("webhookreceiver: signature_header is required")
	}
	switch c.SignatureScheme {
	case SchemeStripe, SchemeAdyen, SchemeGenericHMACSHA256:
	default:
		return fmt.Errorf("webhookreceiver: unsupported signature_scheme %q", c.SignatureScheme)
	}
	if c.SourceFormat == "" {
		return fmt.Errorf("webhookreceiver: source_format is required")
	}
	return nil
}

// Connector implements connector.SourceConnector. One instance is meant
// to be shared across every tenant's webhook-receiver connector
// configs - ServeHTTP routes by {tenant_id}/{connector_id} from the URL
// path, looking up the matching Config registered via Fetch.
type Connector struct {
	Secrets SecretResolver

	mu      sync.RWMutex
	configs map[uuid.UUID]tenantConfig

	records chan connector.RawRecord

	recentMu sync.Mutex
	recent   map[string]time.Time // deliveryKey -> first-seen time, for transport-layer retry suppression
}

type tenantConfig struct {
	TenantID uuid.UUID
	Config   Config
}

func New(secrets SecretResolver) *Connector {
	return &Connector{
		Secrets: secrets,
		configs: make(map[uuid.UUID]tenantConfig),
		records: make(chan connector.RawRecord, 1000),
		recent:  make(map[string]time.Time),
	}
}

func (c *Connector) SupportsStreaming() bool { return true }

func (c *Connector) Checkpoint() (connector.Cursor, error) {
	// Push-based, nothing to checkpoint - there's no "position" in an
	// inbound HTTP stream to resume from.
	return connector.Cursor{}, nil
}

func (c *Connector) Validate(cfg connector.ConnectorConfig) error {
	var s Config
	if err := json.Unmarshal(cfg.Settings, &s); err != nil {
		return fmt.Errorf("webhookreceiver: invalid settings: %w", err)
	}
	return s.validate()
}

// Fetch registers cfg so ServeHTTP can route incoming requests to it by
// connector ID, then returns the shared records channel. Returns
// immediately (does not block until ctx is done) - unlike a polling
// connector's Fetch, there's no fetch loop to run here; delivery happens
// asynchronously via ServeHTTP for as long as the returned channel is
// being drained.
func (c *Connector) Fetch(ctx context.Context, cfg connector.ConnectorConfig) (<-chan connector.RawRecord, error) {
	var s Config
	if err := json.Unmarshal(cfg.Settings, &s); err != nil {
		return nil, fmt.Errorf("webhookreceiver: invalid settings: %w", err)
	}
	if err := s.validate(); err != nil {
		return nil, err
	}
	c.mu.Lock()
	c.configs[cfg.ConnectorID] = tenantConfig{TenantID: cfg.TenantID, Config: s}
	c.mu.Unlock()
	return c.records, nil
}

// ServeHTTP handles POST /v1/webhooks/ingest/{tenant_id}/{connector_id}.
// tenantID/connectorID are passed in explicitly (extracted from the URL
// path by whatever router mounts this handler - net/http 1.22+'s
// ServeMux path wildcards or chi, either works) rather than parsed here,
// keeping this handler router-agnostic.
func (c *Connector) ServeHTTP(w http.ResponseWriter, r *http.Request, tenantID, connectorID uuid.UUID) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	c.mu.RLock()
	tc, ok := c.configs[connectorID]
	c.mu.RUnlock()
	if !ok || tc.TenantID != tenantID {
		http.Error(w, "unknown connector", http.StatusNotFound)
		return
	}
	cfg := tc.Config

	body, err := io.ReadAll(io.LimitReader(r.Body, maxBodyBytes+1))
	if err != nil {
		http.Error(w, "failed to read body", http.StatusBadRequest)
		return
	}
	if len(body) > maxBodyBytes {
		http.Error(w, "body too large", http.StatusRequestEntityTooLarge)
		return
	}

	// Verify HMAC signature BEFORE touching the body for anything else
	// (plans/task/core/18 Implementation Notes) - resolving the secret
	// and computing the HMAC is the only thing that happens before this
	// check; no parsing, logging, or storing of body content yet.
	secret, err := c.Secrets.Resolve(r.Context(), cfg.HMACSecretRef)
	if err != nil {
		http.Error(w, "signature verification unavailable", http.StatusInternalServerError)
		return
	}
	sig := r.Header.Get(cfg.SignatureHeader)
	if !VerifySignature(cfg.SignatureScheme, secret, body, sig) {
		http.Error(w, "invalid signature", http.StatusUnauthorized)
		return
	}

	// Transport-layer retry suppression: dedup by provider delivery-ID
	// header (if present) combined with body hash, per
	// plans/task/core/18 Implementation Notes. This is a fast,
	// bounded, in-memory first line of defense against a sending
	// gateway's own immediate retry-storm (the connector.RawRecord
	// type carries no header/delivery-ID field to thread further
	// downstream, so this package also exports NaturalKeyFunc, a
	// dedup.NaturalKeyFunc built from the raw body, as the
	// authoritative persistent guard via task 09's SAME
	// ingestion_dedup mechanism - not a second dedup path, just this
	// path's two layers).
	deliveryKey := computeDeliveryKey(r.Header.Get(cfg.DeliveryIDHeader), body)
	if c.seenRecently(deliveryKey) {
		w.WriteHeader(http.StatusOK)
		return
	}

	rec := connector.RawRecord{
		TenantID:     tenantID,
		ConnectorID:  connectorID,
		SourceFormat: cfg.SourceFormat,
		Payload:      body,
		ReceivedAt:   time.Now(),
		BatchID:      uuid.New(),
		SourceMode:   domain.SourceModeStream,
	}

	// Enqueue and return 200 immediately - processing (parsing/mapping/
	// validation) happens asynchronously off this request path, so a
	// slow downstream pipeline can never cause the sending gateway to
	// time out and retry-storm (plans/task/core/18 Implementation
	// Notes: "aim <5s").
	select {
	case c.records <- rec:
		w.WriteHeader(http.StatusOK)
	default:
		// Buffer full - signal backpressure so the gateway retries
		// later rather than silently dropping a financial event.
		http.Error(w, "receiver busy, retry later", http.StatusServiceUnavailable)
	}
}

var _ connector.SourceConnector = (*Connector)(nil)
