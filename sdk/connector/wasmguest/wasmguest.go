// Package wasmguest is the TinyGo-buildable helper package connector
// authors import (plans/task/core/25): it hides the WASM ABI plumbing
// (pointer/length marshaling, host-function imports) behind a plain Go
// API, so an author implements api.SourceConnector normally and calls
// wasmguest.Register(myConnector) in main() - no unsafe.Pointer code in
// connector authors' own code.
//
// Host functions are imported via //go:wasmimport (TinyGo supports
// this directive, matching stock Go 1.21+'s own wasip1 support - this
// package targets TinyGo per the design's explicit choice (§3.1:
// sandboxing + smaller binaries than stock Go's wasip1 output), not
// because stock Go can't run here). Guest exports use TinyGo's own
// //export convention (plans/task/core/25's own Implementation Notes
// show this exact form) rather than stock Go's newer //go:wasmexport -
// TinyGo predates that directive and has its own compiler pass for
// //export.
package wasmguest

import (
	"context"
	"encoding/json"
	"unsafe"

	"github.com/koriebruh/jengine-connector-sdk/api"
)

var registered api.SourceConnector

// Register wires c's Fetch/Validate/SupportsStreaming into the guest
// exports the host (internal/ingestion/wasmrunner in the main module)
// invokes. Call this once from main() - see sdk/connector/cmd/jengine-connector's
// own scaffold template for the expected shape.
func Register(c api.SourceConnector) {
	registered = c
}

// allocBuf is where jengine_alloc points the host at for writing
// config bytes before jengine_fetch/jengine_validate - a single fixed
// buffer is sufficient here since the host writes synchronously right
// before each call and this guest is single-threaded per invocation.
var allocBuf [65536]byte

//export jengine_alloc
func jengineAlloc(size uint32) uint32 {
	return uint32(uintptr(unsafe.Pointer(&allocBuf[0])))
}

// jengine_fetch ranges over your connector's Fetch channel and calls
// EmitRecord per item. Found via direct testing against a real
// TinyGo-compiled module: if your Fetch implementation populates its
// returned channel from a spawned goroutine rather than synchronously
// before returning, TinyGo's WASI goroutine scheduler can corrupt guest
// state in a way that surfaces as an unrelated nil-map panic here, not
// at the actual fault site - populate the channel synchronously (see
// the scaffold's own Fetch template and its doc comment).
//
//export jengine_fetch
func jengineFetch(configPtr, configLen uint32) int32 {
	cfg := ptrToBytes(configPtr, configLen)
	var cc api.ConnectorConfig
	if len(cfg) > 0 {
		if err := json.Unmarshal(cfg, &cc); err != nil {
			Log(2, "jengine_fetch: invalid config JSON: "+err.Error())
			return -4
		}
	}

	if registered == nil {
		Log(2, "jengine_fetch: no connector registered")
		return -4
	}

	ch, err := registered.Fetch(context.Background(), cc)
	if err != nil {
		Log(2, "jengine_fetch: Fetch failed: "+err.Error())
		return -4
	}
	for rec := range ch {
		b, err := json.Marshal(rec)
		if err != nil {
			Log(2, "jengine_fetch: failed to marshal record: "+err.Error())
			continue
		}
		if !EmitRecord(b) {
			Log(2, "jengine_fetch: emit_record rejected a record")
		}
	}
	return 0
}

//export jengine_validate
func jengineValidate(configPtr, configLen uint32) int32 {
	cfg := ptrToBytes(configPtr, configLen)
	var cc api.ConnectorConfig
	if len(cfg) > 0 {
		if err := json.Unmarshal(cfg, &cc); err != nil {
			return -4
		}
	}
	if registered == nil {
		return -4
	}
	if err := registered.Validate(cc); err != nil {
		return -4
	}
	return 0
}

//export jengine_supports_streaming
func jengineSupportsStreaming() uint32 {
	if registered != nil && registered.SupportsStreaming() {
		return 1
	}
	return 0
}

// ptrToBytes reconstructs a byte slice from a raw WASM linear-memory
// offset the host passed in - go vet's unsafeptr check flags the
// uintptr->unsafe.Pointer conversion here (a pointer built from a bare
// integer, not derived from an existing Go object), which is usually a
// real bug elsewhere but is exactly what a WASM guest ABI requires:
// the host only ever hands over an i32 memory offset, never a real Go
// pointer. Safe here because wazero (host side) never places guest
// memory addresses outside the guest's own linear memory.
func ptrToBytes(ptr, length uint32) []byte {
	if length == 0 {
		return nil
	}
	return unsafe.Slice((*byte)(unsafe.Pointer(uintptr(ptr))), length) //nolint:govet // see doc comment above
}

// --- Host function imports + Go-friendly wrappers ---

//go:wasmimport jengine_host emit_record
func hostEmitRecord(ptr, length uint32) int32

// EmitRecord sends one parsed record to the host - call once per
// record inside your Fetch implementation.
func EmitRecord(data []byte) bool {
	if len(data) == 0 {
		return true
	}
	ptr := uint32(uintptr(unsafe.Pointer(&data[0])))
	return hostEmitRecord(ptr, uint32(len(data))) >= 0
}

//go:wasmimport jengine_host get_secret
func hostGetSecret(keyPtr, keyLen, outPtr, outLen uint32) int32

// GetSecret resolves a secret by key - the key MUST be declared in
// your connector's Manifest.AllowedSecretKeys, or the host denies the
// request regardless of whether it could technically resolve a value
// for that key.
func GetSecret(key string) (string, bool) {
	kb := []byte(key)
	out := make([]byte, 4096)
	var kp uint32
	if len(kb) > 0 {
		kp = uint32(uintptr(unsafe.Pointer(&kb[0])))
	}
	op := uint32(uintptr(unsafe.Pointer(&out[0])))
	n := hostGetSecret(kp, uint32(len(kb)), op, uint32(len(out)))
	if n < 0 {
		return "", false
	}
	return string(out[:n]), true
}

//go:wasmimport jengine_host log
func hostLog(level int32, msgPtr, msgLen uint32)

// Log sends a structured log line to the host, bridged to its own
// slog. NEVER pass a raw secret value here - cert-scan's dynamic check
// specifically watches for secret-shaped strings reaching this
// function during a harness test run.
func Log(level int32, msg string) {
	mb := []byte(msg)
	if len(mb) == 0 {
		return
	}
	mp := uint32(uintptr(unsafe.Pointer(&mb[0])))
	hostLog(level, mp, uint32(len(mb)))
}

//go:wasmimport jengine_host checkpoint_save
func hostCheckpointSave(ptr, length uint32) int32

// CheckpointSave persists cursor as this connector's resumption
// watermark.
func CheckpointSave(cursor []byte) bool {
	if len(cursor) == 0 {
		return true
	}
	p := uint32(uintptr(unsafe.Pointer(&cursor[0])))
	return hostCheckpointSave(p, uint32(len(cursor))) >= 0
}

//go:wasmimport jengine_host checkpoint_load
func hostCheckpointLoad(outPtr, outLen uint32) int32

// CheckpointLoad retrieves the last saved cursor, or ok=false if none
// exists yet (first run).
func CheckpointLoad() (cursor []byte, ok bool) {
	out := make([]byte, 4096)
	op := uint32(uintptr(unsafe.Pointer(&out[0])))
	n := hostCheckpointLoad(op, uint32(len(out)))
	if n < 0 {
		return nil, false
	}
	return out[:n], true
}

//go:wasmimport jengine_host http_fetch
func hostHTTPFetch(reqPtr, reqLen, outPtr, outLen uint32) int32

// HTTPFetch makes a sandboxed outbound HTTP request - the host enforces
// a per-tenant/per-connector egress allowlist BEFORE dispatching it;
// the request never reaches the target if its domain isn't declared in
// your connector's Manifest.AllowedEgressDomains. req is
// "<url>\n<body>" (kept deliberately simple at the ABI boundary - build
// richer HTTP semantics on top of this in your own connector code if
// you need them).
func HTTPFetch(req []byte) ([]byte, bool) {
	if len(req) == 0 {
		return nil, false
	}
	out := make([]byte, 65536)
	rp := uint32(uintptr(unsafe.Pointer(&req[0])))
	op := uint32(uintptr(unsafe.Pointer(&out[0])))
	n := hostHTTPFetch(rp, uint32(len(req)), op, uint32(len(out)))
	if n < 0 {
		return nil, false
	}
	return out[:n], true
}
