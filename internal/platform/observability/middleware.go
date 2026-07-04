package observability

import (
	"context"
	"time"

	"connectrpc.com/connect"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

// NewConnectInterceptor returns a connect.Interceptor that starts a span
// per RPC call and records golden-signal metrics (request count, error
// count, duration) - the single place spans/metrics for the API layer
// are started, so individual handlers never need to hand-instrument
// (plans/task/core/16 Common Pitfalls: scattering OTel calls across
// handlers leads to inconsistent coverage). The span/metric context
// flows through every in-process call the handler makes (repositories,
// task 11's compiler, task 13's LifecycleService) since they all share
// this same ctx.
func NewConnectInterceptor(tracerName string, m *Metrics) connect.Interceptor {
	return NewConnectInterceptorWithTracer(otel.Tracer(tracerName), m)
}

// NewConnectInterceptorWithTracer is NewConnectInterceptor's tracer-
// injectable form, factored out so tests can supply an in-memory
// tracer.Tracer instead of depending on the process-global
// otel.SetTracerProvider state.
func NewConnectInterceptorWithTracer(tracer trace.Tracer, m *Metrics) connect.Interceptor {
	return connect.UnaryInterceptorFunc(func(next connect.UnaryFunc) connect.UnaryFunc {
		return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
			procedure := req.Spec().Procedure
			ctx, span := tracer.Start(ctx, procedure, trace.WithSpanKind(trace.SpanKindServer))
			defer span.End()

			start := time.Now()
			resp, err := next(ctx, req)
			duration := time.Since(start).Seconds()

			attrs := metric.WithAttributes(attribute.String("rpc.procedure", procedure))
			m.RequestCount.Add(ctx, 1, attrs)
			m.RequestDuration.Record(ctx, duration, attrs)

			if err != nil {
				span.RecordError(err)
				span.SetStatus(codes.Error, err.Error())
				m.ErrorCount.Add(ctx, 1, metric.WithAttributes(
					attribute.String("rpc.procedure", procedure),
					attribute.String("rpc.code", connect.CodeOf(err).String()),
				))
			} else {
				span.SetStatus(codes.Ok, "")
			}
			return resp, err
		}
	})
}

// WrapBatchJob wraps a batch partition-processing function fn with a
// fresh ROOT span (batch jobs are not caused by a single traced HTTP
// request - plans/task/core/16 Implementation Notes: starting a new
// root span per job "is fine and expected," not a shortcut to fix) plus
// golden-signal metrics (job count by outcome, duration, records
// processed, partition size).
func WrapBatchJob(ctx context.Context, tracerName, jobName string, m *Metrics, recordCount int, fn func(ctx context.Context) error) error {
	return WrapBatchJobWithTracer(ctx, otel.Tracer(tracerName), jobName, m, recordCount, fn)
}

// WrapBatchJobWithTracer is WrapBatchJob's tracer-injectable form, for
// the same testability reason as NewConnectInterceptorWithTracer.
func WrapBatchJobWithTracer(ctx context.Context, tracer trace.Tracer, jobName string, m *Metrics, recordCount int, fn func(ctx context.Context) error) error {
	ctx, span := tracer.Start(ctx, jobName, trace.WithNewRoot(), trace.WithSpanKind(trace.SpanKindInternal))
	defer span.End()

	attrs := metric.WithAttributes(attribute.String("job.name", jobName))
	m.PartitionSize.Record(ctx, int64(recordCount), attrs)

	start := time.Now()
	err := fn(ctx)
	duration := time.Since(start).Seconds()

	m.JobDuration.Record(ctx, duration, attrs)
	m.RecordsProcessed.Add(ctx, int64(recordCount), attrs)

	outcome := "success"
	if err != nil {
		outcome = "error"
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	} else {
		span.SetStatus(codes.Ok, "")
	}
	m.JobCount.Add(ctx, 1, metric.WithAttributes(
		attribute.String("job.name", jobName), attribute.String("outcome", outcome),
	))

	return err
}
