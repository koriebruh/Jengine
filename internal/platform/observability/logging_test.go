package observability_test

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"testing"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"

	"github.com/koriebruh/Jengine/internal/platform/observability"
)

func newTestLogger(buf *bytes.Buffer) *slog.Logger {
	base := slog.NewJSONHandler(buf, nil)
	return slog.New(observability.NewTraceContextHandler(base))
}

func TestTraceContextHandler_InjectsTraceAndSpanID_WhenSpanActive(t *testing.T) {
	var buf bytes.Buffer
	logger := newTestLogger(&buf)

	tp := sdktrace.NewTracerProvider()
	defer func() { _ = tp.Shutdown(context.Background()) }()
	tracer := tp.Tracer("test")

	ctx, span := tracer.Start(context.Background(), "test-span")
	defer span.End()

	logger.InfoContext(ctx, "hello")

	var got map[string]any
	if err := json.Unmarshal(buf.Bytes(), &got); err != nil {
		t.Fatalf("unmarshal log line failed: %v (raw: %s)", err, buf.String())
	}

	traceID, ok := got["trace_id"].(string)
	if !ok || traceID == "" {
		t.Errorf("expected a non-empty trace_id field, got: %v", got["trace_id"])
	}
	spanID, ok := got["span_id"].(string)
	if !ok || spanID == "" {
		t.Errorf("expected a non-empty span_id field, got: %v", got["span_id"])
	}
	if traceID != span.SpanContext().TraceID().String() {
		t.Errorf("expected trace_id %q, got %q", span.SpanContext().TraceID().String(), traceID)
	}
}

func TestTraceContextHandler_OmitsTraceFields_WhenNoSpanActive(t *testing.T) {
	var buf bytes.Buffer
	logger := newTestLogger(&buf)

	// No panic, and no trace_id/span_id fields, for a plain background
	// context with no active span.
	logger.InfoContext(context.Background(), "hello")

	var got map[string]any
	if err := json.Unmarshal(buf.Bytes(), &got); err != nil {
		t.Fatalf("unmarshal log line failed: %v (raw: %s)", err, buf.String())
	}
	if _, present := got["trace_id"]; present {
		t.Errorf("expected no trace_id field without an active span, got: %v", got["trace_id"])
	}
	if _, present := got["span_id"]; present {
		t.Errorf("expected no span_id field without an active span, got: %v", got["span_id"])
	}
}
