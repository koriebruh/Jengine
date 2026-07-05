package domain

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

// CaseRoutingConfigStatus mirrors MatchRuleStatus's own value set - same
// draft/active/archived versioning convention (plans/task/core/20).
type CaseRoutingConfigStatus string

const (
	CaseRoutingConfigStatusDraft    CaseRoutingConfigStatus = "DRAFT"
	CaseRoutingConfigStatusActive   CaseRoutingConfigStatus = "ACTIVE"
	CaseRoutingConfigStatusArchived CaseRoutingConfigStatus = "ARCHIVED"
)

// CaseRoutingConfig mirrors case_routing_configs - tenant-scoped
// auto-assignment routing policy consulted by
// internal/cases/workflow's AutoAssignActivity
// (plans/docs/05-case-management.md §6.2). Config is opaque
// json.RawMessage here - interpreting the "strategy"/"team_members"
// shape is internal/cases/workflow's job, not this type's.
type CaseRoutingConfig struct {
	ID        uuid.UUID
	TenantID  uuid.UUID
	Version   int
	Status    CaseRoutingConfigStatus
	Config    json.RawMessage
	CreatedBy string
	CreatedAt time.Time
}
