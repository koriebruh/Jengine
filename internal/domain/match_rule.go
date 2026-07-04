package domain

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
)

type MatchRuleStatus string

const (
	MatchRuleStatusDraft    MatchRuleStatus = "DRAFT"
	MatchRuleStatusActive   MatchRuleStatus = "ACTIVE"
	MatchRuleStatusArchived MatchRuleStatus = "ARCHIVED"
)

type MatchRuleType string

const (
	MatchRuleTypeExact     MatchRuleType = "EXACT"
	MatchRuleTypeTolerance MatchRuleType = "TOLERANCE"
	MatchRuleTypeFuzzy     MatchRuleType = "FUZZY"
	MatchRuleTypeComposite MatchRuleType = "COMPOSITE"
)

// MatchRule mirrors the match_rules table. RuleSpec is opaque
// json.RawMessage here - interpreting it is plans/task/core/11's job, not
// this task's (see Non-Goals).
type MatchRule struct {
	ID                 uuid.UUID
	TenantID           uuid.UUID
	Name               string
	Version            int
	Status             MatchRuleStatus
	RuleSpec           json.RawMessage
	MatchType          MatchRuleType
	SourceAccountID    *uuid.UUID
	TargetAccountID    *uuid.UUID
	Priority           int
	AutoMatchThreshold decimal.Decimal
	CreatedBy          string
	ApprovedBy         *string
	EffectiveFrom      *time.Time
	CreatedAt          time.Time
	UpdatedAt          time.Time
}
