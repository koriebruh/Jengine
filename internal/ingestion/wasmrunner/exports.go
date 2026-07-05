package wasmrunner

import (
	"context"
	"fmt"
)

// Guest export/allocator names - the SDK's wasmguest package (sdk/connector/wasmguest)
// is the one place these are defined on the guest side; mirrored here as
// the host's calling convention.
const (
	exportAlloc             = "jengine_alloc"
	exportFetch             = "jengine_fetch"
	exportValidate          = "jengine_validate"
	exportSupportsStreaming = "jengine_supports_streaming"
)

// call invokes a guest export by name with args, applying cfg.Timeout
// as a wall-clock preemption window. wazero has no true CPU-fuel
// metering (documented, not overclaimed): this runs the guest call in
// a goroutine and races it against a timer, hard-cancelling via
// module closure on timeout.
//
// Found via direct testing, not just theorized: closure-based
// cancellation reliably interrupts a guest loop that calls back into
// the host (a host function call is where wazero's compiled code
// checks whether its module has been closed) - confirmed against this
// package's own timeout test, whose fixture loops calling log() each
// iteration. A guest loop with NO host-function calls or guest-memory
// operations at all (a truly empty `for {}}`) was observed to NOT be
// interrupted by module closure within any bounded window during this
// package's own testing - the goroutine driving that call kept running
// (confirmed via a goroutine dump) well past a 2-minute hard test
// timeout. Do not assume Close() bounds worst-case guest CPU
// consumption for that pathological shape; it only reliably bounds
// guests that interact with the host at some cadence, which is the
// realistic case for anything actually trying to fetch/parse/emit
// records rather than a hand-crafted adversarial busy-loop.
func (r *Runner) call(ctx context.Context, name string, args ...uint64) ([]uint64, error) {
	fn := r.module.ExportedFunction(name)
	if fn == nil {
		return nil, fmt.Errorf("wasmrunner: guest module has no export %q", name)
	}

	callCtx := ctx
	var cancel context.CancelFunc
	if r.cfg.Timeout > 0 {
		callCtx, cancel = context.WithTimeout(ctx, r.cfg.Timeout)
		defer cancel()
	}

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
		// Closing the module hard-cancels the in-flight call from the
		// host's side - wazero surfaces the closed-module error to the
		// still-running goroutine's fn.Call, which then exits (the
		// underlying interpreter/compiled code checks for module
		// closure at safepoints - NOT true preemption of a tight loop
		// with no safepoints, hence the timeout-window caveat above).
		_ = r.module.Close(context.Background())
		return nil, fmt.Errorf("wasmrunner: guest export %q exceeded timeout %s: %w", name, r.cfg.Timeout, callCtx.Err())
	}
}

// writeToGuest allocates guestLen bytes in the guest's own linear
// memory (calling its jengine_alloc export) and writes data there,
// returning the pointer - the guest owns/frees this memory per its own
// allocator's contract (the SDK's wasmguest package documents this).
func (r *Runner) writeToGuest(ctx context.Context, data []byte) (ptr uint32, err error) {
	if len(data) == 0 {
		return 0, nil
	}
	vals, err := r.call(ctx, exportAlloc, uint64(len(data)))
	if err != nil {
		return 0, fmt.Errorf("wasmrunner: guest alloc failed: %w", err)
	}
	if len(vals) < 1 {
		return 0, fmt.Errorf("wasmrunner: guest alloc returned no value")
	}
	p := uint32(vals[0])
	if !r.module.Memory().Write(p, data) {
		return 0, fmt.Errorf("wasmrunner: write to guest memory at ptr=%d failed", p)
	}
	return p, nil
}

// Fetch invokes jengine_fetch(configPtr, configLen) - mirrors
// SourceConnector.Fetch (task 06) from the host's perspective: after
// this returns, EmittedRecords() holds every RawRecord the guest
// produced via emit_record during the call.
func (r *Runner) Fetch(ctx context.Context, config []byte) ([][]byte, error) {
	r.emitted = nil
	ptr, err := r.writeToGuest(ctx, config)
	if err != nil {
		return nil, err
	}
	vals, err := r.call(ctx, exportFetch, uint64(ptr), uint64(len(config)))
	if err != nil {
		return nil, fmt.Errorf("wasmrunner: jengine_fetch failed: %w", err)
	}
	if len(vals) > 0 && int32(vals[0]) != resultOK {
		return r.emitted, fmt.Errorf("wasmrunner: jengine_fetch returned error code %d", int32(vals[0]))
	}
	return r.emitted, nil
}

// Validate invokes jengine_validate(configPtr, configLen) - mirrors
// SourceConnector.Validate.
func (r *Runner) Validate(ctx context.Context, config []byte) error {
	ptr, err := r.writeToGuest(ctx, config)
	if err != nil {
		return err
	}
	vals, err := r.call(ctx, exportValidate, uint64(ptr), uint64(len(config)))
	if err != nil {
		return fmt.Errorf("wasmrunner: jengine_validate failed: %w", err)
	}
	if len(vals) > 0 && int32(vals[0]) != resultOK {
		return fmt.Errorf("wasmrunner: config invalid (code %d)", int32(vals[0]))
	}
	return nil
}

// SupportsStreaming invokes jengine_supports_streaming() - mirrors
// SourceConnector.SupportsStreaming.
func (r *Runner) SupportsStreaming(ctx context.Context) (bool, error) {
	vals, err := r.call(ctx, exportSupportsStreaming)
	if err != nil {
		return false, fmt.Errorf("wasmrunner: jengine_supports_streaming failed: %w", err)
	}
	if len(vals) == 0 {
		return false, fmt.Errorf("wasmrunner: jengine_supports_streaming returned no value")
	}
	return vals[0] != 0, nil
}
