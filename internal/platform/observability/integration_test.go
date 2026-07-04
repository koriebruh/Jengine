package observability_test

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"testing"

	"connectrpc.com/connect"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/koriebruh/Jengine/internal/platform/observability"
)

// TestInterceptorAndLogger_ShareMatchingTraceID is plans/task/core/16's
// DoD integration test: a request through the Connect-RPC interceptor
// produces a span, and a log line emitted from inside the handler (using
// the SAME ctx the interceptor passed through) carries a matching
// trace_id - proving the two mechanisms are actually wired together, not
// just independently correct in isolation.
func TestInterceptorAndLogger_ShareMatchingTraceID(t *testing.T) {
	spanRecorder := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(spanRecorder))
	defer func() { _ = tp.Shutdown(context.Background()) }()

	m, _ := newTestMetrics(t)
	interceptor := observability.NewConnectInterceptorWithTracer(tp.Tracer("test"), m)

	var logBuf bytes.Buffer
	logger := slog.New(observability.NewTraceContextHandler(slog.NewJSONHandler(&logBuf, nil)))

	next := connect.UnaryFunc(func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
		logger.InfoContext(ctx, "handling request")
		return connect.NewResponse(&struct{}{}), nil
	})
	wrapped := interceptor.WrapUnary(next)

	req := fakeAnyRequest{spec: connect.Spec{Procedure: "/test.Service/Method"}}
	if _, err := wrapped(context.Background(), req); err != nil {
		t.Fatalf("wrapped call failed: %v", err)
	}

	spans := spanRecorder.Ended()
	if len(spans) != 1 {
		t.Fatalf("expected 1 span, got %d", len(spans))
	}
	spanTraceID := spans[0].SpanContext().TraceID().String()

	var logLine map[string]any
	if err := json.Unmarshal(logBuf.Bytes(), &logLine); err != nil {
		t.Fatalf("unmarshal log line failed: %v (raw: %s)", err, logBuf.String())
	}
	logTraceID, _ := logLine["trace_id"].(string)
	if logTraceID == "" {
		t.Fatal("expected the log line to carry a trace_id")
	}
	if logTraceID != spanTraceID {
		t.Errorf("expected log trace_id %q to match span trace_id %q", logTraceID, spanTraceID)
	}
}
