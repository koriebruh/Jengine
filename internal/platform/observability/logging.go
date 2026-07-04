package observability

import (
	"context"
	"log/slog"
	"os"

	"go.opentelemetry.io/otel/trace"
)

// traceContextHandler wraps a slog.Handler, injecting trace_id/span_id
// attributes on every record when a valid span is active in ctx
// (plans/task/core/16 Implementation Notes: "so a support engineer can
// jump from a log line to the matching trace without manual
// correlation"). WithAttrs/WithGroup are overridden (not just inherited
// via embedding) so a derived logger via .With(...)/.WithGroup(...)
// keeps the trace-injection behavior instead of silently losing it to
// the unwrapped inner handler.
type traceContextHandler struct {
	inner slog.Handler
}

func (h *traceContextHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return h.inner.Enabled(ctx, level)
}

func (h *traceContextHandler) Handle(ctx context.Context, r slog.Record) error {
	if sc := trace.SpanContextFromContext(ctx); sc.IsValid() {
		r.AddAttrs(
			slog.String("trace_id", sc.TraceID().String()),
			slog.String("span_id", sc.SpanID().String()),
		)
	}
	return h.inner.Handle(ctx, r)
}

func (h *traceContextHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &traceContextHandler{inner: h.inner.WithAttrs(attrs)}
}

func (h *traceContextHandler) WithGroup(name string) slog.Handler {
	return &traceContextHandler{inner: h.inner.WithGroup(name)}
}

// NewTraceContextHandler wraps inner with trace_id/span_id injection -
// exported so it's independently testable/reusable against any base
// slog.Handler, not just the JSON one NewLogger wires up by default.
func NewTraceContextHandler(inner slog.Handler) slog.Handler {
	return &traceContextHandler{inner: inner}
}

// NewLogger builds a JSON-structured slog.Logger (log/slog, stdlib -
// plans/task/core/16 Non-Goals: no zerolog/zap, one dependency-light
// logging mechanism for the whole codebase) whose every log record
// carries trace_id/span_id when written from a context with an active
// span, and omits them cleanly (no panic, no empty-string fields) when
// there isn't one.
func NewLogger(cfg Config) *slog.Logger {
	base := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo})
	handler := NewTraceContextHandler(base)
	return slog.New(handler).With(
		slog.String("service", cfg.ServiceName),
		slog.String("environment", cfg.Environment),
	)
}

var _ slog.Handler = (*traceContextHandler)(nil)
