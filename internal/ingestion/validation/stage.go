package validation

import (
	"context"
	"strings"

	"github.com/google/uuid"

	"github.com/koriebruh/Jengine/internal/ingestion/pipeline"
)

// ValidationStage implements pipeline stage 5 (Validation,
// plans/task/core/09): schema checks then business rules, both against
// task 08's NormalizedFields (not raw un-normalized fields - the
// pipeline order Field Mapping -> Normalization -> Validation is
// deliberate, plans/task/core/09 Common Pitfalls). Any failure ->
// StageQuarantine with a reason joining every failed check, not just the
// first.
type ValidationStage struct {
	AccountID         uuid.UUID
	BusinessValidator *BusinessValidator // nil is valid - schema-only validation
}

func (s *ValidationStage) Name() string { return "validation" }

func (s *ValidationStage) Process(ctx context.Context, rec *pipeline.PipelineRecord) (pipeline.StageResult, error) {
	var errs []ValidationError
	errs = append(errs, ValidateSchema(rec.Normalized)...)

	if s.BusinessValidator != nil {
		rc := BusinessRuleContext{AccountID: s.AccountID}
		errs = append(errs, s.BusinessValidator.Validate(ctx, rc, rec.Normalized)...)
	}

	if len(errs) == 0 {
		return pipeline.StageContinue, nil
	}

	reasons := make([]string, len(errs))
	for i, e := range errs {
		reasons[i] = e.String()
	}
	return pipeline.StageQuarantine, &QuarantineError{Reasons: strings.Join(reasons, "; ")}
}

// QuarantineError wraps a joined set of validation failure reasons - a
// distinct type (not a bare fmt.Errorf) so callers can distinguish "this
// record failed validation" from other error classes if needed later.
type QuarantineError struct {
	Reasons string
}

func (e *QuarantineError) Error() string { return "validation failed: " + e.Reasons }

var _ pipeline.Stage = (*ValidationStage)(nil)
