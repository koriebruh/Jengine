// Package testharness replays a sample raw file/payload through a
// compiled WASM connector module without a running Jengine instance
// (plans/task/core/25): mock pipeline, dry-run mapping preview, panic/
// timeout assertions, and golden-fixture diffing - the same conformance-
// test philosophy already used for the built-in MT940/BAI2/ISO20022
// format parsers (plans/docs/16-development-workflow.md §16.4).
//
// This package has its OWN wazero-based execution logic rather than
// importing internal/ingestion/wasmrunner from the main Jengine module:
// sdk/connector is a separate Go module specifically so a third party
// depending on it never needs the main module as a transitive
// dependency, and Go's own visibility rules make importing an
// internal/ package from outside its module impossible regardless.
// The host-function wiring here is intentionally a subset of the real
// runner's, sufficient for local conformance testing, not a substitute
// for it in production.
package testharness

import (
	"context"
	"fmt"
	"time"

	"github.com/tetratelabs/wazero"
	"github.com/tetratelabs/wazero/api"
	"github.com/tetratelabs/wazero/imports/wasi_snapshot_preview1"
)

// MockSecrets is an in-memory SecretResolver for local testing - no
// real Vault, matching this harness's own "without a running Jengine
// instance" framing.
type MockSecrets map[string]string

// MockCheckpoint is an in-memory, single-run CheckpointStore.
type MockCheckpoint struct{ cursor []byte }

// Harness loads and exercises one compiled WASM connector module.
type Harness struct {
	runtime wazero.Runtime
	module  api.Module
	emitted [][]byte
	logs    []string
	secrets MockSecrets
	checkpt *MockCheckpoint
	timeout time.Duration
	allowed map[string]bool // egress allowlist for this harness run
}

// Options configures a Harness run.
type Options struct {
	Secrets              MockSecrets
	AllowedEgressDomains []string
	Timeout              time.Duration
}

// New loads wasmBytes and wires up the mock jengine_host module.
func New(ctx context.Context, wasmBytes []byte, opts Options) (*Harness, error) {
	if opts.Timeout == 0 {
		opts.Timeout = 10 * time.Second
	}
	h := &Harness{
		secrets: opts.Secrets,
		checkpt: &MockCheckpoint{},
		timeout: opts.Timeout,
		allowed: map[string]bool{},
	}
	for _, d := range opts.AllowedEgressDomains {
		h.allowed[d] = true
	}

	h.runtime = wazero.NewRuntime(ctx)
	if _, err := wasi_snapshot_preview1.Instantiate(ctx, h.runtime); err != nil {
		_ = h.runtime.Close(ctx)
		return nil, fmt.Errorf("testharness: instantiate WASI: %w", err)
	}
	if err := h.buildHostModule(ctx); err != nil {
		_ = h.runtime.Close(ctx)
		return nil, fmt.Errorf("testharness: build host module: %w", err)
	}

	compiled, err := h.runtime.CompileModule(ctx, wasmBytes)
	if err != nil {
		_ = h.runtime.Close(ctx)
		return nil, fmt.Errorf("testharness: compile module: %w", err)
	}
	mod, err := h.runtime.InstantiateModule(ctx, compiled, wazero.NewModuleConfig())
	if err != nil {
		_ = h.runtime.Close(ctx)
		return nil, fmt.Errorf("testharness: instantiate module: %w", err)
	}
	h.module = mod

	if initFn := mod.ExportedFunction("_initialize"); initFn != nil {
		if _, err := initFn.Call(ctx); err != nil {
			_ = h.runtime.Close(ctx)
			return nil, fmt.Errorf("testharness: guest _initialize failed: %w", err)
		}
	}

	return h, nil
}

func (h *Harness) Close(ctx context.Context) error {
	return h.runtime.Close(ctx)
}

// EmittedRecords returns every record the guest emitted during the
// most recent Fetch call.
func (h *Harness) EmittedRecords() [][]byte { return h.emitted }

// Logs returns every message the guest sent via log() during the most
// recent call - used by cert-scan's dynamic secret-leak check.
func (h *Harness) Logs() []string { return h.logs }
