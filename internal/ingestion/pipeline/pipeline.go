package pipeline

import (
	"context"
	"fmt"
	"runtime"

	"github.com/google/uuid"
	"golang.org/x/sync/errgroup"

	"github.com/koriebruh/Jengine/internal/ingestion/connector"
)

// QuarantineSink is the minimal interface Pipeline needs to route
// StageQuarantine records - satisfied by internal/ingestion.QuarantineSink;
// declared locally (structural typing) so this package doesn't need to
// import internal/ingestion, which would import this package back to
// construct a Pipeline - avoiding that cycle.
type QuarantineSink interface {
	Quarantine(ctx context.Context, tenantID, connectorID uuid.UUID, stage, reason string, payload []byte) error
}

// Pipeline runs an ordered list of Stages (Format Parse through
// Persist+Emit - stages 2-8 of plans/docs/02-data-ingestion.md §3.2;
// stage 1, Raw Fetch, is the connector.SourceConnector.Fetch channel this
// pipeline consumes from) over every record a connector produces, with a
// bounded worker pool (plans/task/core/06 Implementation Notes - never
// unbounded goroutine-per-record).
type Pipeline struct {
	Stages         []Stage
	Quarantine     QuarantineSink
	WorkerPoolSize int // default runtime.GOMAXPROCS(0)*2 if <= 0

	// OnRecordProcessed, if set, is called once per record after it
	// finishes moving through Stages (whichever way it ended) - used by
	// tests to assert per-record stage order
	// (plans/task/core/06 DoD) and available for real observability
	// hooks later. Called concurrently from worker goroutines; must be
	// safe for concurrent use if set.
	OnRecordProcessed func(RecordOutcome)
}

// RecordOutcome is what happened to one record as it moved through
// Stages - StageOrder records which stage names actually ran, in order,
// so tests can assert ordering without depending on final output shape
// alone.
type RecordOutcome struct {
	Quarantined bool
	Dropped     bool
	StageOrder  []string
	Err         error
}

// Run consumes every record from conn.Fetch, processes each through
// Stages with a bounded worker pool, and returns once the connector's
// channel closes and all in-flight records finish. A single record's
// failure (quarantine or error) never stops processing of sibling
// records (plans/docs/15-end-to-end-flows.md §15.5).
func (p *Pipeline) Run(ctx context.Context, conn connector.SourceConnector, cfg connector.ConnectorConfig) error {
	ch, err := conn.Fetch(ctx, cfg)
	if err != nil {
		return fmt.Errorf("pipeline: fetch: %w", err)
	}

	poolSize := p.WorkerPoolSize
	if poolSize <= 0 {
		poolSize = runtime.GOMAXPROCS(0) * 2
	}

	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(poolSize)

	for raw := range ch {
		raw := raw
		g.Go(func() error {
			// A record-level failure is intentionally swallowed here (not
			// returned to errgroup) so one bad record can't cancel gctx
			// and abort sibling records still in flight - see
			// plans/docs/15-end-to-end-flows.md §15.5's "one bad record
			// never halts the pipeline" principle. Only a context
			// cancellation (caller-driven) actually stops the pipeline.
			outcome := p.ProcessOne(gctx, raw)
			if p.OnRecordProcessed != nil {
				p.OnRecordProcessed(outcome)
			}
			return nil
		})
	}

	return g.Wait()
}

// ProcessOne runs one raw record through every Stage, in order, stopping
// early on StageQuarantine/StageDrop. Exported so tests can drive a
// single record synchronously without going through Run's concurrent
// sweep.
func (p *Pipeline) ProcessOne(ctx context.Context, raw connector.RawRecord) RecordOutcome {
	rec := &PipelineRecord{Raw: raw}
	outcome := RecordOutcome{}

	for _, stage := range p.Stages {
		outcome.StageOrder = append(outcome.StageOrder, stage.Name())

		result, err := stage.Process(ctx, rec)
		if err != nil {
			rec.Errors = append(rec.Errors, StageError{Stage: stage.Name(), Message: err.Error()})
			outcome.Err = err
		}

		switch result {
		case StageContinue:
			continue
		case StageQuarantine:
			outcome.Quarantined = true
			reason := "quarantined at stage " + stage.Name()
			if err != nil {
				reason = err.Error()
			}
			if p.Quarantine != nil {
				_ = p.Quarantine.Quarantine(ctx, raw.TenantID, raw.ConnectorID, stage.Name(), reason, raw.Payload)
			}
			return outcome
		case StageDrop:
			outcome.Dropped = true
			return outcome
		}
	}

	return outcome
}
