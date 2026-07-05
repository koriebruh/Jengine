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
	// MatchResultStatusAutoMatchedStreaming is PROVISIONAL - written by
	// the streaming matching worker (plans/task/core/19). Must never be
	// treated as a final/closed match anywhere (API responses, webhooks,
	// dashboards) without a "provisional, pending batch confirmation"
	// qualifier - the nightly batch pass is the authoritative source of
	// truth that reconciles it into one of:
	MatchResultStatusAutoMatchedStreaming MatchResultStatus = "AUTO_MATCHED_STREAMING"
	// MatchResultStatusAutoMatchedConfirmed is FINAL - the batch/
	// streaming reconciliation job's outcome when a streaming match is
	// concordant with the authoritative batch pass over the same data.
	MatchResultStatusAutoMatchedConfirmed MatchResultStatus = "AUTO_MATCHED_CONFIRMED"
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
