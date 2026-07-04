package domain

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

type MappingSpecStatus string

const (
	MappingSpecStatusDraft    MappingSpecStatus = "DRAFT"
	MappingSpecStatusActive   MappingSpecStatus = "ACTIVE"
	MappingSpecStatusArchived MappingSpecStatus = "ARCHIVED"
)

// MappingSpec mirrors the mapping_specs table - the persisted, versioned
// record wrapping a serialized internal/ingestion/mapping.MappingSpec
// (the parsed DSL structure). Named the same as that DSL type
// deliberately (like domain.MatchRule holds RuleSpec json.RawMessage
// rather than a parsed AST type) - package-qualified references
// (domain.MappingSpec vs mapping.MappingSpec) disambiguate fully.
type MappingSpec struct {
	ID           uuid.UUID
	TenantID     uuid.UUID
	SourceFormat string
	Version      int
	Status       MappingSpecStatus
	Spec         json.RawMessage
	CreatedAt    time.Time
	UpdatedAt    time.Time
}
