package core

import (
	"context"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
)

// OpenBreakParams is what a caller needs to open a Break/Case for
// unmatched residue - see plans/docs/03-canonical-data-model.md §4.1's
// Break/Case entity.
type OpenBreakParams struct {
	TenantID       uuid.UUID
	AccountID      uuid.UUID
	TransactionIDs []uuid.UUID
	BreakType      string // UNMATCHED|AMOUNT_MISMATCH|TIMING_DIFFERENCE|DUPLICATE|FX_VARIANCE|MISSING_COUNTERPARTY
	AmountAtRisk   decimal.Decimal
	Currency       string
}

// BreakSink is the interface boundary Match's caller (cmd/matching-batch,
// plans/task/core/12) uses to turn MatchOutcome.Unmatched residue into
// Break rows - never called from inside Match itself (Match only
// returns the residue; deciding what to do with it, and any persistence,
// is the caller's job, plans/task/core/10 Common Pitfalls). The concrete
// implementation lives in internal/cases (plans/task/core/13) and is
// dependency-injected in cmd/matching-batch/main.go, the only place
// allowed to import both internal/matching/core and internal/cases -
// neither this package nor internal/matching/rules may ever import
// internal/cases (plans/docs/16-development-workflow.md §16.1).
type BreakSink interface {
	OpenBreak(ctx context.Context, params OpenBreakParams) error
}
