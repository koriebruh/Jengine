package observability

import (
	"fmt"

	"go.opentelemetry.io/otel/metric"
)

// Metrics holds every instrument this codebase emits: golden signals
// (request rate/error rate/duration - the per-Connect-RPC-method and
// per-batch-job shapes) plus the business metrics
// plans/docs/10-observability-reliability.md §11.2 names explicitly.
type Metrics struct {
	// Golden signals - Connect-RPC (per method, status code).
	RequestCount    metric.Int64Counter
	ErrorCount      metric.Int64Counter
	RequestDuration metric.Float64Histogram

	// Golden signals - batch jobs (plans/task/core/12).
	JobCount         metric.Int64Counter
	JobDuration      metric.Float64Histogram
	RecordsProcessed metric.Int64Counter
	// PartitionSize is a histogram, not a gauge - useful for catching
	// the "50k working-set cap not enforced" pitfall plans/task/core/12
	// itself calls out: a histogram shows the actual distribution over
	// time, a single gauge would only ever show the LATEST partition's
	// size.
	PartitionSize metric.Int64Histogram

	// Business metrics.
	MatchAutoMatchedTotal               metric.Int64Counter
	MatchSuggestedTotal                 metric.Int64Counter
	MatchUnmatchedTotal                 metric.Int64Counter
	BreakOpenedTotal                    metric.Int64Counter
	BreakResolvedTotal                  metric.Int64Counter
	AuditChainVerificationFailuresTotal metric.Int64Counter
}

// NewMetrics creates every instrument from the given meter (typically
// otel.Meter(serviceName), called after InitMeterProvider has registered
// the SDK MeterProvider globally).
func NewMetrics(meter metric.Meter) (*Metrics, error) {
	m := &Metrics{}
	var err error

	if m.RequestCount, err = meter.Int64Counter("rpc_request_count",
		metric.WithDescription("Total Connect-RPC requests received, by method and status")); err != nil {
		return nil, fmt.Errorf("observability: rpc_request_count: %w", err)
	}
	if m.ErrorCount, err = meter.Int64Counter("rpc_error_count",
		metric.WithDescription("Total Connect-RPC requests that returned an error, by method and code")); err != nil {
		return nil, fmt.Errorf("observability: rpc_error_count: %w", err)
	}
	if m.RequestDuration, err = meter.Float64Histogram("rpc_request_duration_seconds",
		metric.WithDescription("Connect-RPC request duration"), metric.WithUnit("s")); err != nil {
		return nil, fmt.Errorf("observability: rpc_request_duration_seconds: %w", err)
	}

	if m.JobCount, err = meter.Int64Counter("batch_job_count",
		metric.WithDescription("Total batch partition jobs processed, by outcome")); err != nil {
		return nil, fmt.Errorf("observability: batch_job_count: %w", err)
	}
	if m.JobDuration, err = meter.Float64Histogram("batch_job_duration_seconds",
		metric.WithDescription("Batch partition job duration"), metric.WithUnit("s")); err != nil {
		return nil, fmt.Errorf("observability: batch_job_duration_seconds: %w", err)
	}
	if m.RecordsProcessed, err = meter.Int64Counter("batch_records_processed_total",
		metric.WithDescription("Total transaction records processed across all batch jobs")); err != nil {
		return nil, fmt.Errorf("observability: batch_records_processed_total: %w", err)
	}
	if m.PartitionSize, err = meter.Int64Histogram("batch_partition_size",
		metric.WithDescription("Record count per batch partition (source+target combined)")); err != nil {
		return nil, fmt.Errorf("observability: batch_partition_size: %w", err)
	}

	if m.MatchAutoMatchedTotal, err = meter.Int64Counter("match_auto_matched_total",
		metric.WithDescription("Total candidates auto-matched, by tenant and rule")); err != nil {
		return nil, fmt.Errorf("observability: match_auto_matched_total: %w", err)
	}
	if m.MatchSuggestedTotal, err = meter.Int64Counter("match_suggested_total",
		metric.WithDescription("Total candidates surfaced as suggested matches, by tenant and rule")); err != nil {
		return nil, fmt.Errorf("observability: match_suggested_total: %w", err)
	}
	if m.MatchUnmatchedTotal, err = meter.Int64Counter("match_unmatched_total",
		metric.WithDescription("Total transactions left unmatched after all rules ran, by tenant")); err != nil {
		return nil, fmt.Errorf("observability: match_unmatched_total: %w", err)
	}
	if m.BreakOpenedTotal, err = meter.Int64Counter("break_opened_total",
		metric.WithDescription("Total breaks opened, by tenant")); err != nil {
		return nil, fmt.Errorf("observability: break_opened_total: %w", err)
	}
	if m.BreakResolvedTotal, err = meter.Int64Counter("break_resolved_total",
		metric.WithDescription("Total breaks resolved, by tenant and root cause")); err != nil {
		return nil, fmt.Errorf("observability: break_resolved_total: %w", err)
	}
	if m.AuditChainVerificationFailuresTotal, err = meter.Int64Counter("audit_chain_verification_failures_total",
		metric.WithDescription("Total tampered/broken audit chains detected by task 14's verification job, by tenant")); err != nil {
		return nil, fmt.Errorf("observability: audit_chain_verification_failures_total: %w", err)
	}

	return m, nil
}
