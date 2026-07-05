// Package kafkasource implements the Kafka-topic-ingestion
// SourceConnector (plans/task/core/18): a tenant points their own
// external Kafka cluster at Jengine, this connector consumes it via a
// committed consumer group. The tenant's messages are NOT assumed to be
// Jengine's internal TransactionEvent protobuf - they go through the
// same tenant-configured field-mapping DSL (plans/task/core/08) as any
// other source format; this connector's job is transport + auth only.
package kafkasource

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"

	"github.com/koriebruh/Jengine/internal/domain"
	"github.com/koriebruh/Jengine/internal/ingestion/connector"
)

// SecretResolver resolves a Vault path reference to its secret value -
// same shape/rationale as every other connector package's own local
// copy (see connector/sftp.SecretResolver, connector/webhookreceiver.SecretResolver).
type SecretResolver interface {
	Resolve(ctx context.Context, vaultPathRef string) (string, error)
}

// Config is this connector's ConnectorConfig.Settings shape, mirroring
// plans/task/core/18's KafkaConnectorConfig sketch.
type Config struct {
	BootstrapServers []string `json:"bootstrap_servers"`
	Topic            string   `json:"topic"`
	ConsumerGroup    string   `json:"consumer_group"`
	AuthMode         string   `json:"auth_mode"`     // "SASL_SSL" | "mTLS" | "PLAINTEXT" (PLAINTEXT: local dev/test only)
	SchemaFormat     string   `json:"schema_format"` // "json" | "avro" | "protobuf" - the TENANT's own schema, not Jengine's internal one
	SourceFormat     string   `json:"source_format"` // field-mapping DSL's source_format key

	// AuthMode == SASL_SSL
	SASLUsernameRef string `json:"sasl_username_ref,omitempty"`
	SASLPasswordRef string `json:"sasl_password_ref,omitempty"`
	SASLMechanism   string `json:"sasl_mechanism,omitempty"` // "PLAIN" | "SCRAM-SHA-256" | "SCRAM-SHA-512"

	// AuthMode == mTLS
	TLSCertRef string `json:"tls_cert_ref,omitempty"`
	TLSKeyRef  string `json:"tls_key_ref,omitempty"`
	TLSCARef   string `json:"tls_ca_ref,omitempty"`
}

const (
	AuthModePlaintext = "PLAINTEXT"
	AuthModeSASLSSL   = "SASL_SSL"
	AuthModeMTLS      = "mTLS"
)

func (c Config) validate() error {
	if len(c.BootstrapServers) == 0 {
		return fmt.Errorf("kafkasource: bootstrap_servers is required")
	}
	if c.Topic == "" {
		return fmt.Errorf("kafkasource: topic is required")
	}
	if c.ConsumerGroup == "" {
		return fmt.Errorf("kafkasource: consumer_group is required")
	}
	if c.SourceFormat == "" {
		return fmt.Errorf("kafkasource: source_format is required")
	}
	switch c.AuthMode {
	case AuthModePlaintext, AuthModeSASLSSL, AuthModeMTLS:
	default:
		return fmt.Errorf("kafkasource: unsupported auth_mode %q", c.AuthMode)
	}
	if c.AuthMode == AuthModeSASLSSL && (c.SASLUsernameRef == "" || c.SASLPasswordRef == "") {
		return fmt.Errorf("kafkasource: sasl_username_ref/sasl_password_ref required for SASL_SSL")
	}
	if c.AuthMode == AuthModeMTLS && (c.TLSCertRef == "" || c.TLSKeyRef == "") {
		return fmt.Errorf("kafkasource: tls_cert_ref/tls_key_ref required for mTLS")
	}
	return nil
}

// Connector implements connector.SourceConnector, one instance per
// Fetch call (unlike webhookreceiver's single shared instance) - each
// tenant's own external Kafka cluster needs its own client connection
// and consumer loop.
type Connector struct {
	Secrets SecretResolver

	mu             sync.Mutex
	client         *kgo.Client
	lastPartitions map[int32]int64 // partition -> last-seen offset, for Checkpoint() observability
}

func New(secrets SecretResolver) *Connector {
	return &Connector{Secrets: secrets, lastPartitions: make(map[int32]int64)}
}

func (c *Connector) SupportsStreaming() bool { return true }

// Checkpoint reports the last-seen offset per partition. This is
// observational, not the actual resume mechanism - Kafka's own
// committed consumer-group offsets are what a restart resumes from
// (plans/task/core/18 Implementation Notes: "Checkpoint via committed
// consumer-group offsets"). Exposed here so redrive/replay tooling can
// inspect progress uniformly across connector types, matching task 06's
// Cursor contract, even though resetting it doesn't control resume
// position the way e.g. connector/sftp's Cursor does - resetting Kafka
// consumption requires operating on the consumer group directly (kafka
// tooling), not this struct.
func (c *Connector) Checkpoint() (connector.Cursor, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	state, err := json.Marshal(c.lastPartitions)
	if err != nil {
		return connector.Cursor{}, fmt.Errorf("kafkasource: marshal checkpoint state: %w", err)
	}
	return connector.Cursor{State: state, UpdatedAt: time.Now()}, nil
}

func (c *Connector) Validate(cfg connector.ConnectorConfig) error {
	var s Config
	if err := json.Unmarshal(cfg.Settings, &s); err != nil {
		return fmt.Errorf("kafkasource: invalid settings: %w", err)
	}
	return s.validate()
}

// Fetch connects to the tenant's Kafka cluster and starts a background
// consume loop, returning a channel of RawRecords translated 1:1 from
// the tenant's own topic messages (opaque payload, no schema
// interpretation here - SchemaFormat is metadata for the field-mapping
// stage, not something this connector parses). The loop runs until ctx
// is canceled.
func (c *Connector) Fetch(ctx context.Context, cfg connector.ConnectorConfig) (<-chan connector.RawRecord, error) {
	var s Config
	if err := json.Unmarshal(cfg.Settings, &s); err != nil {
		return nil, fmt.Errorf("kafkasource: invalid settings: %w", err)
	}
	if err := s.validate(); err != nil {
		return nil, err
	}

	opts, err := c.buildClientOpts(ctx, s)
	if err != nil {
		return nil, err
	}
	client, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("kafkasource: new client: %w", err)
	}

	c.mu.Lock()
	c.client = client
	c.mu.Unlock()

	out := make(chan connector.RawRecord)
	go c.consumeLoop(ctx, client, cfg, s, out)
	return out, nil
}

func (c *Connector) consumeLoop(ctx context.Context, client *kgo.Client, cfg connector.ConnectorConfig, s Config, out chan<- connector.RawRecord) {
	defer close(out)
	defer client.Close()

	for {
		if ctx.Err() != nil {
			return
		}
		fetches := client.PollFetches(ctx)
		if ctx.Err() != nil {
			return
		}
		fetches.EachError(func(_ string, _ int32, err error) {
			// Non-fatal per-partition fetch errors (e.g. transient
			// leader election) - franz-go retries internally; this
			// loop keeps polling rather than treating any single
			// error as terminal.
		})

		fetches.EachRecord(func(rec *kgo.Record) {
			select {
			case out <- connector.RawRecord{
				TenantID:     cfg.TenantID,
				ConnectorID:  cfg.ConnectorID,
				SourceFormat: s.SourceFormat,
				Payload:      rec.Value,
				ReceivedAt:   time.Now(),
				BatchID:      uuid.New(),
				SourceMode:   domain.SourceModeStream,
			}:
				c.mu.Lock()
				c.lastPartitions[rec.Partition] = rec.Offset
				c.mu.Unlock()
			case <-ctx.Done():
				return
			}
		})

		// Idempotent-consumer model per plans/docs/06-streaming-architecture.md
		// §7.3: commit AFTER the record has been handed to the
		// pipeline (which itself dedupes by idempotency key), not
		// before - at-least-once delivery, safe under a crash between
		// send and commit because a redelivered record is absorbed by
		// downstream dedup, never silently lost.
		if err := client.CommitUncommittedOffsets(ctx); err != nil && ctx.Err() == nil {
			// A commit failure just means the next poll may redeliver
			// already-processed records - downstream dedup (task 09)
			// absorbs this; not a reason to stop consuming.
			continue
		}
	}
}

func (c *Connector) buildClientOpts(ctx context.Context, s Config) ([]kgo.Opt, error) {
	opts := []kgo.Opt{
		kgo.SeedBrokers(s.BootstrapServers...),
		kgo.ConsumerGroup(s.ConsumerGroup),
		kgo.ConsumeTopics(s.Topic),
		kgo.DisableAutoCommit(),
	}

	switch s.AuthMode {
	case AuthModePlaintext:
		// no additional opts - unencrypted, unauthenticated. Only
		// valid for local dev/testing against a trusted network.

	case AuthModeSASLSSL:
		username, err := c.Secrets.Resolve(ctx, s.SASLUsernameRef)
		if err != nil {
			return nil, fmt.Errorf("kafkasource: resolve sasl username: %w", err)
		}
		password, err := c.Secrets.Resolve(ctx, s.SASLPasswordRef)
		if err != nil {
			return nil, fmt.Errorf("kafkasource: resolve sasl password: %w", err)
		}
		mechanism, err := saslMechanism(s.SASLMechanism, username, password)
		if err != nil {
			return nil, err
		}
		opts = append(opts, kgo.SASL(mechanism), kgo.DialTLSConfig(&tls.Config{MinVersion: tls.VersionTLS12}))

	case AuthModeMTLS:
		certPEM, err := c.Secrets.Resolve(ctx, s.TLSCertRef)
		if err != nil {
			return nil, fmt.Errorf("kafkasource: resolve tls cert: %w", err)
		}
		keyPEM, err := c.Secrets.Resolve(ctx, s.TLSKeyRef)
		if err != nil {
			return nil, fmt.Errorf("kafkasource: resolve tls key: %w", err)
		}
		cert, err := tls.X509KeyPair([]byte(certPEM), []byte(keyPEM))
		if err != nil {
			return nil, fmt.Errorf("kafkasource: parse client cert/key: %w", err)
		}
		tlsCfg := &tls.Config{MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{cert}}
		if s.TLSCARef != "" {
			caPEM, err := c.Secrets.Resolve(ctx, s.TLSCARef)
			if err != nil {
				return nil, fmt.Errorf("kafkasource: resolve tls ca: %w", err)
			}
			pool := x509.NewCertPool()
			if !pool.AppendCertsFromPEM([]byte(caPEM)) {
				return nil, fmt.Errorf("kafkasource: invalid CA certificate")
			}
			tlsCfg.RootCAs = pool
		}
		opts = append(opts, kgo.DialTLSConfig(tlsCfg))
	}

	return opts, nil
}

func saslMechanism(mechanism, username, password string) (sasl.Mechanism, error) {
	switch mechanism {
	case "", "PLAIN":
		return plain.Auth{User: username, Pass: password}.AsMechanism(), nil
	case "SCRAM-SHA-256":
		return scram.Auth{User: username, Pass: password}.AsSha256Mechanism(), nil
	case "SCRAM-SHA-512":
		return scram.Auth{User: username, Pass: password}.AsSha512Mechanism(), nil
	default:
		return nil, fmt.Errorf("kafkasource: unsupported sasl_mechanism %q", mechanism)
	}
}

var _ connector.SourceConnector = (*Connector)(nil)
