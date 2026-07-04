package domain

import (
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
)

type MatchCardinality string

const (
	MatchCardinalityOneToOne   MatchCardinality = "ONE_TO_ONE"
	MatchCardinalityOneToMany  MatchCardinality = "ONE_TO_MANY"
	MatchCardinalityManyToOne  MatchCardinality = "MANY_TO_ONE"
	MatchCardinalityManyToMany MatchCardinality = "MANY_TO_MANY"
)

type MatchResultStatus string

const (
	MatchResultStatusAutoMatched MatchResultStatus = "AUTO_MATCHED"
	MatchResultStatusSuggested   MatchResultStatus = "SUGGESTED"
	MatchResultStatusConfirmed   MatchResultStatus = "CONFIRMED"
	MatchResultStatusRejected    MatchResultStatus = "REJECTED"
)

// MatchResult mirrors match_results. Kept separate from MatchResultLine
// (1-to-N, not collapsed) per plans/task/core/05 Common Pitfalls - the
// matching engine allocates partial amounts per line for many-to-many
// matches.
type MatchResult struct {
	ID              uuid.UUID
	TenantID        uuid.UUID
	RuleID          *uuid.UUID
	MatchType       MatchCardinality
	ConfidenceScore decimal.Decimal
	Status          MatchResultStatus
	MatchedAt       time.Time
	MatchedBy       *string
	AmountVariance  *decimal.Decimal
	DateVariance    *int
	CreatedAt       time.Time
}

type MatchResultLineSide string

const (
	MatchResultLineSideSource MatchResultLineSide = "SOURCE"
	MatchResultLineSideTarget MatchResultLineSide = "TARGET"
)

// MatchResultLine mirrors match_result_lines.
type MatchResultLine struct {
	MatchResultID   uuid.UUID
	TransactionID   uuid.UUID
	TenantID        uuid.UUID
	Side            MatchResultLineSide
	AllocatedAmount decimal.Decimal
}
