package observability

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	otelprom "go.opentelemetry.io/otel/exporters/prometheus"
	"go.opentelemetry.io/otel/propagation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.31.0"
)

// Config identifies this process to the observability backend and
// configures where telemetry is exported to.
type Config struct {
	ServiceName    string
	ServiceVersion string
	// OTLPEndpoint is the collector's OTLP/HTTP endpoint (traces only -
	// see InitMeterProvider's doc comment for why metrics use a
	// different mechanism), env-configured, defaulting to the local
	// Jaeger instance task 02's docker-compose `--profile observability`
	// starts (plans/docs/16-development-workflow.md §16.2).
	OTLPEndpoint string
	// Environment is dev|staging|prod, attached as a resource attribute.
	Environment string
	// MetricsAddr is where InitMeterProvider serves a Prometheus-
	// compatible /metrics endpoint for scraping (see InitMeterProvider).
	MetricsAddr string
}

func resourceFor(cfg Config) (*resource.Resource, error) {
	return resource.Merge(resource.Default(), resource.NewSchemaless(
		semconv.ServiceName(cfg.ServiceName),
		semconv.ServiceVersion(cfg.ServiceVersion),
		semconv.DeploymentEnvironmentName(cfg.Environment),
	))
}

// InitTracerProvider builds and registers (via otel.SetTracerProvider)
// an SDK TracerProvider exporting spans over OTLP/HTTP to cfg.OTLPEndpoint.
// Callers must call the returned shutdown function on process exit
// (plans/task/core/16 Common Pitfalls: spans generated right before a
// shutdown are silently dropped without this).
func InitTracerProvider(ctx context.Context, cfg Config) (shutdown func(context.Context) error, err error) {
	res, err := resourceFor(cfg)
	if err != nil {
		return nil, fmt.Errorf("observability: build resource: %w", err)
	}

	exporter, err := otlptracehttp.New(ctx,
		otlptracehttp.WithEndpoint(cfg.OTLPEndpoint),
		otlptracehttp.WithInsecure(), // local collector, plaintext - matches task 02's dev-only compose profile
	)
	if err != nil {
		return nil, fmt.Errorf("observability: new otlp trace exporter: %w", err)
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
	)
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{}, propagation.Baggage{},
	))

	return tp.Shutdown, nil
}

// InitMeterProvider builds and registers (via otel.SetMeterProvider) an
// SDK MeterProvider backed by the Prometheus exporter, and starts an
// HTTP server on cfg.MetricsAddr serving /metrics for Prometheus to
// scrape.
//
// This uses Prometheus-style pull/scrape rather than OTLP push
// (plans/task/core/16 Implementation Notes explicitly allows either,
// "confirm compatibility rather than assuming"): task 02's docker-compose
// `--profile observability` starts a bare `prom/prometheus` with no
// otel-collector in front of it to bridge OTLP metric pushes into
// Prometheus's pull model, so a directly-scrapeable /metrics endpoint is
// the mechanism that's actually compatible with the infra as built, not
// a design preference. Uses its own client_golang Registry (not the
// package-global DefaultRegisterer) to avoid cross-package global-state
// coupling.
func InitMeterProvider(ctx context.Context, cfg Config) (shutdown func(context.Context) error, err error) {
	res, err := resourceFor(cfg)
	if err != nil {
		return nil, fmt.Errorf("observability: build resource: %w", err)
	}

	registry := prometheus.NewRegistry()
	exporter, err := otelprom.New(otelprom.WithRegisterer(registry))
	if err != nil {
		return nil, fmt.Errorf("observability: new prometheus exporter: %w", err)
	}

	mp := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(exporter),
		sdkmetric.WithResource(res),
	)
	otel.SetMeterProvider(mp)

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(registry, promhttp.HandlerOpts{}))
	server := &http.Server{Addr: cfg.MetricsAddr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		_ = server.ListenAndServe()
	}()

	return func(shutdownCtx context.Context) error {
		if err := server.Shutdown(shutdownCtx); err != nil {
			return err
		}
		return mp.Shutdown(shutdownCtx)
	}, nil
}
