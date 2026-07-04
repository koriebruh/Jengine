package pipeline

import (
	"context"

	"github.com/koriebruh/Jengine/internal/ingestion/connector"
)

// NormalizedFields is a placeholder shape for post-normalization data.
// Task 08 defines the real typed struct once the field-mapping DSL's
// output shape is known (plans/task/core/06 Implementation Notes) - this
// exists so PipelineRecord has something concrete before then.
type NormalizedFields map[string]any

// StageError is a non-fatal issue accumulated on a PipelineRecord as it
// moves through stages - a fatal error short-circuits to quarantine
// instead (see StageResult).
type StageError struct {
	Stage   string
	Message string
}

// PipelineRecord is threaded through every stage, progressively enriched.
type PipelineRecord struct {
	Raw          connector.RawRecord
	ParsedFields map[string]any   // after Format Parse (stage 2)
	MappedFields map[string]any   // after Field Mapping (stage 3, task 08's DSL output)
	Normalized   NormalizedFields // after Normalization (stage 4, task 08)
	Errors       []StageError
}

// StageResult tells the pipeline what to do with a record after a stage
// runs.
type StageResult int

const (
	// StageContinue moves the record to the next stage.
	StageContinue StageResult = iota
	// StageQuarantine means the record is invalid - route to the
	// QuarantineSink and stop processing THIS record only; sibling
	// records in the same batch continue unaffected
	// (plans/docs/15-end-to-end-flows.md §15.5).
	StageQuarantine
	// StageDrop is a deliberate skip (e.g. a duplicate under a tenant's
	// "reject resend" policy) - must be logged, never silent
	// (plans/task/core/06 Implementation Notes).
	StageDrop
)

// Stage is one step in the pipeline. Format Parse (task 07),
// Field Mapping and Normalization (task 08), Validation and Dedup
// (task 09) are all implemented as Stages elsewhere and injected into
// Pipeline - this package only defines the seam and orchestrates it,
// per plans/task/core/06 Non-Goals.
type Stage interface {
	Name() string
	Process(ctx context.Context, rec *PipelineRecord) (StageResult, error)
}
