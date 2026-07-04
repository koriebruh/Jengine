package observability_test

import (
	"context"
	"errors"
	"testing"

	"connectrpc.com/connect"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/koriebruh/Jengine/internal/platform/observability"
)

type fakeAnyRequest struct {
	connect.AnyRequest
	spec connect.Spec
}

func (f fakeAnyRequest) Spec() connect.Spec { return f.spec }

func newTestMetrics(t *testing.T) (*observability.Metrics, *sdkmetric.ManualReader) {
	t.Helper()
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	m, err := observability.NewMetrics(mp.Meter("test"))
	if err != nil {
		t.Fatalf("NewMetrics failed: %v", err)
	}
	return m, reader
}

func collectMetric(t *testing.T, reader *sdkmetric.ManualReader, name string) metricdata.Metrics {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("collect metrics failed: %v", err)
	}
	for _, sm := range rm.ScopeMetrics {
		for _, metric := range sm.Metrics {
			if metric.Name == name {
				return metric
			}
		}
	}
	t.Fatalf("metric %q not found", name)
	return metricdata.Metrics{}
}

func TestNewConnectInterceptor_RecordsSpanAndMetrics_SuccessPath(t *testing.T) {
	spanRecorder := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(spanRecorder))
	defer func() { _ = tp.Shutdown(context.Background()) }()

	m, reader := newTestMetrics(t)
	interceptor := observability.NewConnectInterceptorWithTracer(tp.Tracer("test"), m)

	next := connect.UnaryFunc(func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
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
	if spans[0].Name() != "/test.Service/Method" {
		t.Errorf("expected span name '/test.Service/Method', got %q", spans[0].Name())
	}

	reqCount := collectMetric(t, reader, "rpc_request_count")
	sum := reqCount.Data.(metricdata.Sum[int64])
	if len(sum.DataPoints) != 1 || sum.DataPoints[0].Value != 1 {
		t.Errorf("expected rpc_request_count=1, got %+v", sum.DataPoints)
	}
}

func TestNewConnectInterceptor_RecordsErrorMetric_ErrorPath(t *testing.T) {
	spanRecorder := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(spanRecorder))
	defer func() { _ = tp.Shutdown(context.Background()) }()

	m, reader := newTestMetrics(t)
	interceptor := observability.NewConnectInterceptorWithTracer(tp.Tracer("test"), m)

	wantErr := connect.NewError(connect.CodeInternal, errors.New("boom"))
	next := connect.UnaryFunc(func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
		return nil, wantErr
	})
	wrapped := interceptor.WrapUnary(next)

	req := fakeAnyRequest{spec: connect.Spec{Procedure: "/test.Service/Method"}}
	if _, err := wrapped(context.Background(), req); err == nil {
		t.Fatal("expected the error to propagate")
	}

	errCount := collectMetric(t, reader, "rpc_error_count")
	sum := errCount.Data.(metricdata.Sum[int64])
	if len(sum.DataPoints) != 1 || sum.DataPoints[0].Value != 1 {
		t.Errorf("expected rpc_error_count=1, got %+v", sum.DataPoints)
	}

	spans := spanRecorder.Ended()
	if len(spans) != 1 {
		t.Fatalf("expected 1 span, got %d", len(spans))
	}
	if spans[0].Status().Code.String() != "Error" {
		t.Errorf("expected span status Error, got %s", spans[0].Status().Code.String())
	}
}

func TestWrapBatchJob_RecordsSpanAndMetrics(t *testing.T) {
	spanRecorder := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(spanRecorder))
	defer func() { _ = tp.Shutdown(context.Background()) }()

	m, reader := newTestMetrics(t)

	err := observability.WrapBatchJobWithTracer(context.Background(), tp.Tracer("test"), "test-job", m, 42, func(ctx context.Context) error {
		return nil
	})
	if err != nil {
		t.Fatalf("WrapBatchJob failed: %v", err)
	}

	spans := spanRecorder.Ended()
	if len(spans) != 1 || spans[0].Name() != "test-job" {
		t.Fatalf("expected 1 span named 'test-job', got %+v", spans)
	}

	recordsProcessed := collectMetric(t, reader, "batch_records_processed_total")
	sum := recordsProcessed.Data.(metricdata.Sum[int64])
	if len(sum.DataPoints) != 1 || sum.DataPoints[0].Value != 42 {
		t.Errorf("expected batch_records_processed_total=42, got %+v", sum.DataPoints)
	}

	jobErr := errors.New("job failed")
	err = observability.WrapBatchJobWithTracer(context.Background(), tp.Tracer("test"), "test-job-2", m, 5, func(ctx context.Context) error {
		return jobErr
	})
	if !errors.Is(err, jobErr) {
		t.Fatalf("expected the job's own error to propagate, got: %v", err)
	}

	jobCount := collectMetric(t, reader, "batch_job_count")
	sum2 := jobCount.Data.(metricdata.Sum[int64])
	var errorOutcomeFound bool
	for _, dp := range sum2.DataPoints {
		for _, attr := range dp.Attributes.ToSlice() {
			if attr.Key == "outcome" && attr.Value.AsString() == "error" {
				errorOutcomeFound = true
			}
		}
	}
	if !errorOutcomeFound {
		t.Errorf("expected a batch_job_count data point with outcome=error, got %+v", sum2.DataPoints)
	}
}
