package testharness

import (
	"context"
	"fmt"
)

const (
	exportAlloc             = "jengine_alloc"
	exportFetch             = "jengine_fetch"
	exportValidate          = "jengine_validate"
	exportSupportsStreaming = "jengine_supports_streaming"
)

func (h *Harness) call(ctx context.Context, name string, args ...uint64) ([]uint64, error) {
	fn := h.module.ExportedFunction(name)
	if fn == nil {
		return nil, fmt.Errorf("testharness: guest module has no export %q", name)
	}

	callCtx, cancel := context.WithTimeout(ctx, h.timeout)
	defer cancel()

	type callResult struct {
		vals []uint64
		err  error
	}
	resultCh := make(chan callResult, 1)
	go func() {
		vals, err := fn.Call(callCtx, args...)
		resultCh <- callResult{vals: vals, err: err}
	}()

	select {
	case res := <-resultCh:
		return res.vals, res.err
	case <-callCtx.Done():
		// See internal/ingestion/wasmrunner's own doc comment in the
		// main module for the full finding this mirrors: closing the
		// module reliably interrupts a guest that calls back into the
		// host at some cadence, but is not confirmed to bound a guest
		// doing zero host/memory interaction at all.
		_ = h.module.Close(context.Background())
		return nil, fmt.Errorf("testharness: guest export %q exceeded timeout %s: %w", name, h.timeout, callCtx.Err())
	}
}

func (h *Harness) writeToGuest(ctx context.Context, data []byte) (uint32, error) {
	if len(data) == 0 {
		return 0, nil
	}
	vals, err := h.call(ctx, exportAlloc, uint64(len(data)))
	if err != nil {
		return 0, fmt.Errorf("testharness: guest alloc failed: %w", err)
	}
	if len(vals) < 1 {
		return 0, fmt.Errorf("testharness: guest alloc returned no value")
	}
	p := uint32(vals[0])
	if !h.module.Memory().Write(p, data) {
		return 0, fmt.Errorf("testharness: write to guest memory at ptr=%d failed", p)
	}
	return p, nil
}

// Fetch invokes jengine_fetch with config, returning every record the
// guest emitted.
func (h *Harness) Fetch(ctx context.Context, config []byte) ([][]byte, error) {
	h.emitted = nil
	ptr, err := h.writeToGuest(ctx, config)
	if err != nil {
		return nil, err
	}
	vals, err := h.call(ctx, exportFetch, uint64(ptr), uint64(len(config)))
	if err != nil {
		return nil, fmt.Errorf("testharness: jengine_fetch failed: %w", err)
	}
	if len(vals) > 0 && int32(vals[0]) != resultOK {
		return h.emitted, fmt.Errorf("testharness: jengine_fetch returned error code %d", int32(vals[0]))
	}
	return h.emitted, nil
}

// Validate invokes jengine_validate with config.
func (h *Harness) Validate(ctx context.Context, config []byte) error {
	ptr, err := h.writeToGuest(ctx, config)
	if err != nil {
		return err
	}
	vals, err := h.call(ctx, exportValidate, uint64(ptr), uint64(len(config)))
	if err != nil {
		return fmt.Errorf("testharness: jengine_validate failed: %w", err)
	}
	if len(vals) > 0 && int32(vals[0]) != resultOK {
		return fmt.Errorf("testharness: config invalid (code %d)", int32(vals[0]))
	}
	return nil
}

// SupportsStreaming invokes jengine_supports_streaming.
func (h *Harness) SupportsStreaming(ctx context.Context) (bool, error) {
	vals, err := h.call(ctx, exportSupportsStreaming)
	if err != nil {
		return false, fmt.Errorf("testharness: jengine_supports_streaming failed: %w", err)
	}
	if len(vals) == 0 {
		return false, fmt.Errorf("testharness: jengine_supports_streaming returned no value")
	}
	return vals[0] != 0, nil
}
