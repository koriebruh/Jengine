package domain

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
)

type BreakType string

const (
	BreakTypeUnmatched           BreakType = "UNMATCHED"
	BreakTypeAmountMismatch      BreakType = "AMOUNT_MISMATCH"
	BreakTypeTimingDifference    BreakType = "TIMING_DIFFERENCE"
	BreakTypeDuplicate           BreakType = "DUPLICATE"
	BreakTypeFXVariance          BreakType = "FX_VARIANCE"
	BreakTypeMissingCounterparty BreakType = "MISSING_COUNTERPARTY"
	// BreakTypeReconciliationVariance is created by the batch/streaming
	// reconciliation job (plans/task/core/19) when a streaming
	// provisional match and the authoritative batch pass disagree - a
	// deliberately lightweight review case, not a full re-investigation.
	BreakTypeReconciliationVariance BreakType = "RECONCILIATION_VARIANCE"
)

type CaseStatus string

const (
	CaseStatusOpen            CaseStatus = "OPEN"
	CaseStatusAssigned        CaseStatus = "ASSIGNED"
	CaseStatusInProgress      CaseStatus = "IN_PROGRESS"
	CaseStatusPendingApproval CaseStatus = "PENDING_APPROVAL"
	CaseStatusResolved        CaseStatus = "RESOLVED"
	CaseStatusWrittenOff      CaseStatus = "WRITTEN_OFF"
	CaseStatusEscalated       CaseStatus = "ESCALATED"
	CaseStatusReopened        CaseStatus = "REOPENED"
)

// Case mirrors the cases table (the Break/Case entity - table named
// `cases`, not `breaks`, per migrations/0001_init_schema.up.sql).
type Case struct {
	ID                    uuid.UUID
	TenantID              uuid.UUID
	AccountID             uuid.UUID
	RelatedTransactionIDs []uuid.UUID
	BreakType             BreakType
	RootCauseCategory     *string
	Status                CaseStatus
	AssignedTo            *string
	Priority              string
	SLADueAt              *time.Time
	OpenedAt              time.Time
	ResolvedAt            *time.Time
	AmountAtRisk          *decimal.Decimal
	Currency              *string
	TemporalWorkflowID    *string
	CreatedAt             time.Time
	UpdatedAt             time.Time
}

// CaseComment mirrors case_comments - append-only, no UpdatedAt.
type CaseComment struct {
	ID        uuid.UUID
	TenantID  uuid.UUID
	CaseID    uuid.UUID
	Actor     string
	EventType string
	Payload   json.RawMessage
	CreatedAt time.Time
}

// CaseAuditEvent mirrors case_audit_events - append-only.
type CaseAuditEvent struct {
	ID        uuid.UUID
	TenantID  uuid.UUID
	CaseID    uuid.UUID
	Actor     string
	EventType string
	Payload   json.RawMessage
	CreatedAt time.Time
}
