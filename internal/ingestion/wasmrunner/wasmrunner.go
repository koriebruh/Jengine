// Package wasmrunner is the sandbox executor for third-party WASM
// connectors (plans/task/core/25, plans/docs/02-data-ingestion.md §3.1):
// loads a compiled WASM module via wazero, wires the jengine_host
// function surface, invokes the guest's jengine_fetch/jengine_validate/
// jengine_supports_streaming exports, and marshals RawRecords across
// the boundary. This is deliberately NOT native Go plugin execution
// (Go's `plugin` package is explicitly rejected by the design for ABI-
// fragility reasons) - WASM sandboxing is the whole point: untrusted
// third-party connector code gets no filesystem or raw network access
// beyond what the declared host functions below expose.
package wasmrunner

import (
	"context"
	"fmt"
	"time"

	"github.com/tetratelabs/wazero"
	"github.com/tetratelabs/wazero/api"
	"github.com/tetratelabs/wazero/imports/wasi_snapshot_preview1"
)

// SecretResolver resolves a declared secret key to its value - the
// guest never sees raw Vault credentials, only resolved values for
// keys it explicitly declared in Manifest.AllowedSecretKeys (§3.1's
// own framing, mirrored here).
type SecretResolver interface {
	Resolve(ctx context.Context, key string) (string, error)
}

// CheckpointStore persists a connector's resumption cursor between
// runs (checkpoint_save/checkpoint_load host functions).
type CheckpointStore interface {
	Save(ctx context.Context, cursor []byte) error
	Load(ctx context.Context) ([]byte, error)
}

// Manifest is a connector's declared capabilities - the cert-scan
// (task 25's own scaffold CLI subcommand) and the sandbox both enforce
// against it. A connector requesting a secret key or egress domain not
// listed here is rejected, not silently allowed.
type Manifest struct {
	AllowedSecretKeys    []string
	AllowedEgressDomains []string
}

func (m Manifest) secretKeyAllowed(key string) bool {
	for _, k := range m.AllowedSecretKeys {
		if k == key {
			return true
		}
	}
	return false
}

func (m Manifest) egressDomainAllowed(domain string) bool {
	for _, d := range m.AllowedEgressDomains {
		if d == domain {
			return true
		}
	}
	return false
}

// Config bounds a Runner's resource usage - wazero has no true CPU-
// fuel metering (documented limitation, not overclaimed anywhere in
// this package). Timeout is wall-clock preemption via module closure,
// confirmed by direct testing to reliably interrupt a guest loop that
// calls back into the host at some cadence (a host-function call is
// where wazero's compiled code checks for closure) - but NOT confirmed
// to bound a guest doing zero host/memory interaction at all (a truly
// empty `for {}`), which this package's own tests observed running past
// a 2-minute hard test timeout uninterrupted. See exports.go's own
// (*Runner).call doc comment for the full finding.
type Config struct {
	Manifest       Manifest
	Secrets        SecretResolver
	Checkpoint     CheckpointStore
	HTTPDo         func(ctx context.Context, req []byte) ([]byte, error) // sandboxed outbound fetch, domain-checked against Manifest before this is even called
	MaxMemoryPages uint32                                                // wazero linear-memory page cap (64KiB/page) - 0 means wazero's own default
	Timeout        time.Duration                                         // wall-clock preemption window per guest call
	Logger         func(level int32, msg string)
}

// Runner loads and executes one compiled WASM connector module.
type Runner struct {
	cfg     Config
	runtime wazero.Runtime
	module  api.Module
	emitted [][]byte
	secLog  []secretLogAttempt // recorded for cert-scan's dynamic check (task 25's own DoD)
}

// secretLogAttempt records every log() call's raw message for the
// cert-scan dynamic check ("no secret-shaped string is ever passed to
// the log host function") - cert-scan inspects this after a harness
// run, it isn't enforced by the runner itself (a connector logging its
// OWN non-secret diagnostic text is legitimate; only cert-scan's
// pattern-matching decides if a run is clean).
type secretLogAttempt struct {
	Level   int32
	Message string
}

// NewRunner compiles wasmBytes and instantiates it with the
// jengine_host module wired in. The guest is NOT run yet - Fetch/
// Validate/SupportsStreaming each invoke one export.
func NewRunner(ctx context.Context, wasmBytes []byte, cfg Config) (*Runner, error) {
	rtCfg := wazero.NewRuntimeConfig()
	if cfg.MaxMemoryPages > 0 {
		rtCfg = rtCfg.WithMemoryLimitPages(cfg.MaxMemoryPages)
	}
	runtime := wazero.NewRuntimeWithConfig(ctx, rtCfg)

	if _, err := wasi_snapshot_preview1.Instantiate(ctx, runtime); err != nil {
		_ = runtime.Close(ctx)
		return nil, fmt.Errorf("wasmrunner: instantiate WASI: %w", err)
	}

	r := &Runner{cfg: cfg, runtime: runtime}

	if _, err := r.buildHostModule(ctx); err != nil {
		_ = runtime.Close(ctx)
		return nil, fmt.Errorf("wasmrunner: build host module: %w", err)
	}

	compiled, err := runtime.CompileModule(ctx, wasmBytes)
	if err != nil {
		_ = runtime.Close(ctx)
		return nil, fmt.Errorf("wasmrunner: compile module: %w", err)
	}

	// No WithStartFunctions("_start") here deliberately: a WASI
	// "command" module (built as `package main` with a real func main)
	// has one and runs it as part of instantiation by default; a
	// "reactor"-style guest (only named exports, no meaningful main -
	// both TinyGo connector guests and Go's own -buildmode=c-shared
	// wasip1 fixture used by this package's own tests) does not, and
	// forcing the call would fail module instantiation outright for
	// that shape.
	modCfg := wazero.NewModuleConfig()
	mod, err := runtime.InstantiateModule(ctx, compiled, modCfg)
	if err != nil {
		_ = runtime.Close(ctx)
		return nil, fmt.Errorf("wasmrunner: instantiate guest module: %w", err)
	}
	r.module = mod

	// WASI reactor convention: `_initialize` (distinct from `_start`,
	// which is for command modules) bootstraps the guest's own runtime
	// (Go's wasip1 c-shared target needs this before ANY other export
	// is callable - memory allocation/GC panic with "not initialized"
	// otherwise, found via direct testing) - called once here, not
	// exposed as a Runner method, since every guest export call assumes
	// it already ran. Optional: a guest that doesn't export it needs no
	// such bootstrap (TinyGo's own runtime may not require this step).
	if initFn := mod.ExportedFunction("_initialize"); initFn != nil {
		if _, err := initFn.Call(ctx); err != nil {
			_ = runtime.Close(ctx)
			return nil, fmt.Errorf("wasmrunner: guest _initialize failed: %w", err)
		}
	}

	return r, nil
}

func (r *Runner) Close(ctx context.Context) error {
	return r.runtime.Close(ctx)
}

// EmittedRecords returns every RawRecord payload the guest emitted via
// emit_record during the most recent Fetch call.
func (r *Runner) EmittedRecords() [][]byte { return r.emitted }
