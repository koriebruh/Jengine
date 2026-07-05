package wasmrunner

import (
	"context"
	"fmt"
	"net/url"

	"github.com/tetratelabs/wazero/api"
)

// Result codes returned across the guest/host boundary - i32, matching
// the host-function surface's own documented signatures
// (plans/task/core/25 Implementation Notes).
const (
	resultOK             int32 = 0
	resultBufferTooSmall int32 = -1
	resultDenied         int32 = -2
	resultNotFound       int32 = -3
	resultInternal       int32 = -4
)

// buildHostModule registers the jengine_host module's functions -
// emit_record, get_secret, log, checkpoint_save, checkpoint_load,
// http_fetch - the ENTIRE I/O surface a guest connector has. There is
// deliberately no filesystem or raw socket host function: this is what
// makes WASM sandboxing meaningful here rather than cosmetic (this
// task's own Common Pitfalls names a side-channel host function as the
// exact mistake that would defeat it).
func (r *Runner) buildHostModule(ctx context.Context) (api.Module, error) {
	builder := r.runtime.NewHostModuleBuilder("jengine_host")

	builder.NewFunctionBuilder().
		WithFunc(r.hostEmitRecord).
		Export("emit_record")

	builder.NewFunctionBuilder().
		WithFunc(r.hostGetSecret).
		Export("get_secret")

	builder.NewFunctionBuilder().
		WithFunc(r.hostLog).
		Export("log")

	builder.NewFunctionBuilder().
		WithFunc(r.hostCheckpointSave).
		Export("checkpoint_save")

	builder.NewFunctionBuilder().
		WithFunc(r.hostCheckpointLoad).
		Export("checkpoint_load")

	builder.NewFunctionBuilder().
		WithFunc(r.hostHTTPFetch).
		Export("http_fetch")

	return builder.Instantiate(ctx)
}

func readMemory(m api.Module, ptr, length uint32) ([]byte, error) {
	buf, ok := m.Memory().Read(ptr, length)
	if !ok {
		return nil, fmt.Errorf("wasmrunner: out-of-bounds guest memory read at ptr=%d len=%d", ptr, length)
	}
	// Copy - the slice wazero returns aliases guest linear memory
	// directly; the guest could mutate it after this call returns.
	out := make([]byte, len(buf))
	copy(out, buf)
	return out, nil
}

// writeMemory writes data into the guest's pre-allocated out buffer,
// returning the number of bytes written (>= 0) on success or a
// negative result code on failure - the guest needs the actual byte
// count to slice its own buffer correctly (out[:n]), not just a bare
// "0 = ok" signal, matching a getsockopt-style C API convention.
func writeMemory(m api.Module, outPtr, outCap uint32, data []byte) int32 {
	if uint32(len(data)) > outCap {
		return resultBufferTooSmall
	}
	if !m.Memory().Write(outPtr, data) {
		return resultInternal
	}
	return int32(len(data))
}

// hostEmitRecord: emit_record(ptr, len) -> i32. The guest calls this
// once per parsed record; payload is treated as an opaque RawRecord
// byte blob (the design's own protobuf-encoding choice is a guest-side
// concern - this host function doesn't need to decode it, only collect
// it for the pipeline to consume after Fetch returns).
func (r *Runner) hostEmitRecord(ctx context.Context, m api.Module, ptr, length uint32) int32 {
	data, err := readMemory(m, ptr, length)
	if err != nil {
		return resultInternal
	}
	r.emitted = append(r.emitted, data)
	return resultOK
}

// hostGetSecret: get_secret(key_ptr, key_len, out_ptr, out_len) -> i32.
// Only resolves keys the connector's OWN manifest declared
// (Manifest.AllowedSecretKeys) - a key not declared is resultDenied,
// never silently resolved. The guest never receives the Vault path
// reference itself, only the resolved value for a key it named ahead
// of time - this is the mechanism §3.1 describes.
func (r *Runner) hostGetSecret(ctx context.Context, m api.Module, keyPtr, keyLen, outPtr, outCap uint32) int32 {
	key, err := readMemory(m, keyPtr, keyLen)
	if err != nil {
		return resultInternal
	}
	if !r.cfg.Manifest.secretKeyAllowed(string(key)) {
		return resultDenied
	}
	if r.cfg.Secrets == nil {
		return resultNotFound
	}
	value, err := r.cfg.Secrets.Resolve(ctx, string(key))
	if err != nil {
		return resultNotFound
	}
	return writeMemory(m, outPtr, outCap, []byte(value))
}

// hostLog: log(level, msg_ptr, msg_len). Bridged to the host's own
// structured logging - every call is also recorded in r.secLog for
// cert-scan's dynamic check (task 25 Implementation Notes: "a check
// that no secret-shaped string is ever passed to the log host
// function... via the harness intercepting log calls during a test
// run").
func (r *Runner) hostLog(ctx context.Context, m api.Module, level int32, msgPtr, msgLen uint32) {
	msg, err := readMemory(m, msgPtr, msgLen)
	if err != nil {
		return
	}
	r.secLog = append(r.secLog, secretLogAttempt{Level: level, Message: string(msg)})
	if r.cfg.Logger != nil {
		r.cfg.Logger(level, string(msg))
	}
}

// hostCheckpointSave: checkpoint_save(cursor_ptr, cursor_len) -> i32.
func (r *Runner) hostCheckpointSave(ctx context.Context, m api.Module, cursorPtr, cursorLen uint32) int32 {
	cursor, err := readMemory(m, cursorPtr, cursorLen)
	if err != nil {
		return resultInternal
	}
	if r.cfg.Checkpoint == nil {
		return resultNotFound
	}
	if err := r.cfg.Checkpoint.Save(ctx, cursor); err != nil {
		return resultInternal
	}
	return resultOK
}

// hostCheckpointLoad: checkpoint_load(out_ptr, out_len) -> i32.
func (r *Runner) hostCheckpointLoad(ctx context.Context, m api.Module, outPtr, outCap uint32) int32 {
	if r.cfg.Checkpoint == nil {
		return resultNotFound
	}
	cursor, err := r.cfg.Checkpoint.Load(ctx)
	if err != nil {
		return resultInternal
	}
	if cursor == nil {
		return resultNotFound
	}
	return writeMemory(m, outPtr, outCap, cursor)
}

// hostHTTPFetch: http_fetch(req_ptr, req_len, out_ptr, out_len) -> i32.
// req is expected to be a newline-prefixed "<url>\n<body>" blob (kept
// deliberately simple - full HTTP semantics belong in the SDK's own
// guest-side helper, not renegotiated at the host boundary); the host
// enforces the egress allowlist BEFORE ever dispatching the request -
// a domain not in Manifest.AllowedEgressDomains is resultDenied, the
// request is never sent. This is the sandboxed egress path §3.1
// describes: the guest cannot reach arbitrary hosts.
func (r *Runner) hostHTTPFetch(ctx context.Context, m api.Module, reqPtr, reqLen, outPtr, outCap uint32) int32 {
	req, err := readMemory(m, reqPtr, reqLen)
	if err != nil {
		return resultInternal
	}

	target := req
	if i := indexByte(req, '\n'); i >= 0 {
		target = req[:i]
	}
	u, err := url.Parse(string(target))
	if err != nil {
		return resultDenied
	}
	if !r.cfg.Manifest.egressDomainAllowed(u.Hostname()) {
		return resultDenied
	}
	if r.cfg.HTTPDo == nil {
		return resultDenied
	}

	resp, err := r.cfg.HTTPDo(ctx, req)
	if err != nil {
		return resultInternal
	}
	return writeMemory(m, outPtr, outCap, resp)
}

func indexByte(b []byte, c byte) int {
	for i, x := range b {
		if x == c {
			return i
		}
	}
	return -1
}
