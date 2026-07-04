package validation

import (
	"fmt"

	"github.com/koriebruh/Jengine/internal/ingestion/mapping"
	"github.com/koriebruh/Jengine/internal/ingestion/pipeline"
)

// ValidationError names one failed check - Field is a NormalizedFields
// path (e.g. "currency"), not a target DSL path, since schema validation
// runs against already-mapped/normalized data (plans/task/core/09 Common
// Pitfalls: validate NormalizedFields, not raw un-normalized fields).
type ValidationError struct {
	Field  string
	Reason string
}

func (e ValidationError) String() string {
	return fmt.Sprintf("%s: %s", e.Field, e.Reason)
}

// ValidateSchema checks required fields/types/formats on already-
// normalized data (task 08's NormalizedFields). Currency format
// validation reuses mapping.ValidateISO4217 rather than reimplementing
// it (plans/task/core/09 Implementation Notes).
func ValidateSchema(fields pipeline.NormalizedFields) []ValidationError {
	var errs []ValidationError

	if fields.Currency == "" {
		errs = append(errs, ValidationError{Field: "currency", Reason: "required field is empty"})
	} else if err := mapping.ValidateISO4217(fields.Currency); err != nil {
		errs = append(errs, ValidationError{Field: "currency", Reason: err.Error()})
	}

	if fields.ValueDate.IsZero() {
		errs = append(errs, ValidationError{Field: "value_date", Reason: "required field is missing"})
	}

	if fields.Side == "" {
		errs = append(errs, ValidationError{Field: "side", Reason: "required field is empty"})
	}

	return errs
}
