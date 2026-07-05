// Package api is the stable interface third-party connector authors
// implement (plans/task/core/25). It mirrors internal/ingestion/connector.SourceConnector
// (task 06) structurally - RawRecord/ConnectorConfig/Cursor are
// independently defined here, not imported, since sdk/connector is a
// separate Go module (its own go.mod, decoupled release/versioning
// from the main app - a third party depending on this SDK should never
// need the main Jengine module as a transitive dependency) and
// internal/ packages are unimportable from outside their module anyway.
// The whole point of the SDK is that a connector built against this
// interface and compiled to WASM (sdk/connector/wasmguest) looks and
// behaves like a native SourceConnector from the host's perspective -
// see internal/ingestion/wasmrunner in the main module for the sandbox
// executor that runs it.
package api

import (
	"context"
	"encoding/json"
	"time"
)

// SourceConnector is what a third-party connector implements. Native
// Go connectors (task 06/18) satisfy the same shape directly; a WASM
// guest built with sdk/connector/wasmguest satisfies it indirectly
// through the jengine_fetch/jengine_validate/jengine_supports_streaming
// exports wasmrunner invokes on its behalf.
type SourceConnector interface {
	Fetch(ctx context.Context, cfg ConnectorConfig) (<-chan RawRecord, error)
	Validate(cfg ConnectorConfig) error
	SupportsStreaming() bool
	Checkpoint() (Cursor, error)
}

// RawRecord is one unparsed record, before any parsing/mapping/
// normalization - structurally identical to
// internal/ingestion/connector.RawRecord.
type RawRecord struct {
	TenantID     string
	ConnectorID  string
	SourceFormat string
	Payload      []byte
	ReceivedAt   time.Time
	BatchID      string
	SourceMode   string
}

// ConnectorConfig is the tenant-supplied configuration for one
// connector instance.
type ConnectorConfig struct {
	ConnectorID string
	TenantID    string
	Type        string
	Settings    json.RawMessage
	Schedule    string
}

// Cursor is a connector's opaque watermark/offset, persisted between
// runs via the checkpoint_save/checkpoint_load host functions when
// running as a WASM guest.
type Cursor struct {
	ConnectorID string
	State       json.RawMessage
	UpdatedAt   time.Time
}

// Manifest declares a connector's required capabilities - secret keys
// it needs resolved and egress domains it needs to reach. The sandbox
// (wasmrunner.Manifest on the host side) and the cert-scan tool both
// enforce against this; a connector requesting anything not declared
// here is denied at runtime, not silently allowed. Authors ship this
// alongside their compiled .wasm module (typically as manifest.json in
// the scaffold's project layout).
type Manifest struct {
	Name                 string   `json:"name"`
	Version              string   `json:"version"`
	AllowedSecretKeys    []string `json:"allowed_secret_keys"`
	AllowedEgressDomains []string `json:"allowed_egress_domains"`
}
