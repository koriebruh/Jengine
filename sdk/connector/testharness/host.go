package testharness

import (
	"context"
	"fmt"
	"net/url"

	"github.com/tetratelabs/wazero/api"
)

const (
	resultOK             int32 = 0
	resultBufferTooSmall int32 = -1
	resultDenied         int32 = -2
	resultNotFound       int32 = -3
	resultInternal       int32 = -4
)

// buildHostModule registers the jengine_host module against the same
// six-function surface plans/task/core/25 documents - a mock
// implementation (in-memory secrets/checkpoint, allowlisted egress
// with no real network by default) sufficient for local conformance
// testing without a running Jengine instance.
func (h *Harness) buildHostModule(ctx context.Context) error {
	builder := h.runtime.NewHostModuleBuilder("jengine_host")

	builder.NewFunctionBuilder().WithFunc(h.hostEmitRecord).Export("emit_record")
	builder.NewFunctionBuilder().WithFunc(h.hostGetSecret).Export("get_secret")
	builder.NewFunctionBuilder().WithFunc(h.hostLog).Export("log")
	builder.NewFunctionBuilder().WithFunc(h.hostCheckpointSave).Export("checkpoint_save")
	builder.NewFunctionBuilder().WithFunc(h.hostCheckpointLoad).Export("checkpoint_load")
	builder.NewFunctionBuilder().WithFunc(h.hostHTTPFetch).Export("http_fetch")

	_, err := builder.Instantiate(ctx)
	return err
}

func readMemory(m api.Module, ptr, length uint32) ([]byte, error) {
	buf, ok := m.Memory().Read(ptr, length)
	if !ok {
		return nil, fmt.Errorf("testharness: out-of-bounds guest memory read at ptr=%d len=%d", ptr, length)
	}
	out := make([]byte, len(buf))
	copy(out, buf)
	return out, nil
}

func writeMemory(m api.Module, outPtr, outCap uint32, data []byte) int32 {
	if uint32(len(data)) > outCap {
		return resultBufferTooSmall
	}
	if !m.Memory().Write(outPtr, data) {
		return resultInternal
	}
	return int32(len(data))
}

func (h *Harness) hostEmitRecord(ctx context.Context, m api.Module, ptr, length uint32) int32 {
	data, err := readMemory(m, ptr, length)
	if err != nil {
		return resultInternal
	}
	h.emitted = append(h.emitted, data)
	return resultOK
}

func (h *Harness) hostGetSecret(ctx context.Context, m api.Module, keyPtr, keyLen, outPtr, outCap uint32) int32 {
	key, err := readMemory(m, keyPtr, keyLen)
	if err != nil {
		return resultInternal
	}
	value, ok := h.secrets[string(key)]
	if !ok {
		return resultNotFound
	}
	return writeMemory(m, outPtr, outCap, []byte(value))
}

func (h *Harness) hostLog(ctx context.Context, m api.Module, level int32, msgPtr, msgLen uint32) {
	msg, err := readMemory(m, msgPtr, msgLen)
	if err != nil {
		return
	}
	h.logs = append(h.logs, string(msg))
}

func (h *Harness) hostCheckpointSave(ctx context.Context, m api.Module, cursorPtr, cursorLen uint32) int32 {
	cursor, err := readMemory(m, cursorPtr, cursorLen)
	if err != nil {
		return resultInternal
	}
	h.checkpt.cursor = cursor
	return resultOK
}

func (h *Harness) hostCheckpointLoad(ctx context.Context, m api.Module, outPtr, outCap uint32) int32 {
	if h.checkpt.cursor == nil {
		return resultNotFound
	}
	return writeMemory(m, outPtr, outCap, h.checkpt.cursor)
}

// hostHTTPFetch: mock egress - allowlist-checked like the real runner,
// but never dispatches a real HTTP request even for an allowed domain
// (this harness runs "without a running Jengine instance" - a
// connector wanting to test real HTTP behavior should use a local
// mock server and add its address to AllowedEgressDomains, at which
// point this returns resultDenied since MockHTTPResponses isn't wired
// to actually call out; extend this harness if a real dispatch is
// needed for a specific connector's tests).
func (h *Harness) hostHTTPFetch(ctx context.Context, m api.Module, reqPtr, reqLen, outPtr, outCap uint32) int32 {
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
	if !h.allowed[u.Hostname()] {
		return resultDenied
	}
	// Allowed but this harness doesn't dispatch real requests - see
	// doc comment above.
	return resultDenied
}

func indexByte(b []byte, c byte) int {
	for i, x := range b {
		if x == c {
			return i
		}
	}
	return -1
}
