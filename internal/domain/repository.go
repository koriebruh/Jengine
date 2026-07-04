package domain

import (
	"context"
	"time"

	"github.com/google/uuid"
)

// Every repository method's first non-context parameter is tenantID
// uuid.UUID, explicit at every call site - the concrete implementation of
// the "lint rule + defense-in-depth" convention (plans/task/core/04/05):
// even if ctx were somehow tenant-less, the compiler forces a tenantID
// argument to exist, and implementations must use *that* value as the
// actual filter (asserted to match tenancy.MustTenantFromContext(ctx) as
// a defensive equality check - see internal/storage/postgres), not
// silently re-derive it from ctx alone.

type AccountRepository interface {
	Create(ctx context.Context, tenantID uuid.UUID, a Account) (Account, error)
	GetByID(ctx context.Context, tenantID uuid.UUID, id uuid.UUID) (Account, error)
	ListByTenant(ctx context.Context, tenantID uuid.UUID) ([]Account, error)
}

type StatementRepository interface {
	Create(ctx context.Context, tenantID uuid.UUID, s Statement) (Statement, error)
	GetByID(ctx context.Context, tenantID uuid.UUID, id uuid.UUID) (Statement, error)
	UpdateStatus(ctx context.Context, tenantID uuid.UUID, id uuid.UUID, status StatementStatus) error
	ListByAccount(ctx context.Context, tenantID uuid.UUID, accountID uuid.UUID) ([]Statement, error)
	ExistsByChecksum(ctx context.Context, tenantID uuid.UUID, accountID uuid.UUID, checksum string) (bool, error)
}

// TransactionRepository is the data-access backbone for ingestion and
// matching (plans/task/core/06-12).
type TransactionRepository interface {
	Create(ctx context.Context, tenantID uuid.UUID, tx Transaction) (Transaction, error)
	GetByID(ctx context.Context, tenantID uuid.UUID, id uuid.UUID) (Transaction, error)
	ListUnmatched(ctx context.Context, tenantID uuid.UUID, accountID uuid.UUID, valueDateFrom, valueDateTo time.Time) ([]Transaction, error)
	UpdateStatus(ctx context.Context, tenantID uuid.UUID, id uuid.UUID, status TransactionStatus) error
	// BulkInsert is the batch upsert path for ingestion/matching write-back
	// (plans/docs/04-matching-engine.md §5.2 point 5) - implementations
	// must use COPY/multi-row INSERT, never row-by-row (plans/task/core/05
	// Common Pitfalls).
	BulkInsert(ctx context.Context, tenantID uuid.UUID, txs []Transaction) (int, error)
	// ExistsByIdempotencyKey supports plans/task/core/09's dedup path.
	ExistsByIdempotencyKey(ctx context.Context, tenantID uuid.UUID, key string) (bool, error)
}

type MatchRuleRepository interface {
	Create(ctx context.Context, tenantID uuid.UUID, r MatchRule) (MatchRule, error)
	GetByID(ctx context.Context, tenantID uuid.UUID, id uuid.UUID) (MatchRule, error)
	// ListActive returns ACTIVE rules for an account pair, ordered by
	// priority ascending (plans/docs/04-matching-engine.md §5.1 rule
	// chaining: lower priority runs first).
	ListActive(ctx context.Context, tenantID uuid.UUID, sourceAccountID, targetAccountID uuid.UUID) ([]MatchRule, error)
	UpdateStatus(ctx context.Context, tenantID uuid.UUID, id uuid.UUID, status MatchRuleStatus, approvedBy *string) error
}

// MatchResultRepository covers both MatchResult and MatchResultLine
// persistence - they're always written together transactionally (one
// result, many lines), never as a denormalized single access pattern
// (plans/task/core/05 Common Pitfalls).
type MatchResultRepository interface {
	Create(ctx context.Context, tenantID uuid.UUID, result MatchResult, lines []MatchResultLine) (MatchResult, error)
	GetByID(ctx context.Context, tenantID uuid.UUID, id uuid.UUID) (MatchResult, []MatchResultLine, error)
	ListByStatus(ctx context.Context, tenantID uuid.UUID, status MatchResultStatus) ([]MatchResult, error)
	UpdateStatus(ctx context.Context, tenantID uuid.UUID, id uuid.UUID, status MatchResultStatus, matchedBy *string) error
}

// CaseRepository covers Case, CaseComment, and CaseAuditEvent - the three
// are always read/written in the context of one case (plans/task/core/05
// Scope). Lifecycle/state-machine transition logic is plans/task/core/13;
// this is pure CRUD/read-write.
type CaseRepository interface {
	Create(ctx context.Context, tenantID uuid.UUID, c Case) (Case, error)
	GetByID(ctx context.Context, tenantID uuid.UUID, id uuid.UUID) (Case, error)
	ListByStatus(ctx context.Context, tenantID uuid.UUID, status CaseStatus) ([]Case, error)
	UpdateStatus(ctx context.Context, tenantID uuid.UUID, id uuid.UUID, status CaseStatus) error

	AddComment(ctx context.Context, tenantID uuid.UUID, c CaseComment) (CaseComment, error)
	ListComments(ctx context.Context, tenantID uuid.UUID, caseID uuid.UUID) ([]CaseComment, error)

	AddAuditEvent(ctx context.Context, tenantID uuid.UUID, e CaseAuditEvent) (CaseAuditEvent, error)
	ListAuditEvents(ctx context.Context, tenantID uuid.UUID, caseID uuid.UUID) ([]CaseAuditEvent, error)
}

type ConnectorRepository interface {
	Create(ctx context.Context, tenantID uuid.UUID, c Connector) (Connector, error)
	GetByID(ctx context.Context, tenantID uuid.UUID, id uuid.UUID) (Connector, error)
	ListByTenant(ctx context.Context, tenantID uuid.UUID) ([]Connector, error)
	UpdateCursorState(ctx context.Context, tenantID uuid.UUID, id uuid.UUID, cursorState []byte, lastRunAt time.Time) error
}
