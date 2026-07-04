package cases

import (
	"context"

	"github.com/koriebruh/Jengine/internal/matching/core"
)

// BreakSinkAdapter adapts LifecycleService to core.BreakSink - the only
// file in this package that imports internal/matching/core (plans/task/core/13
// Common Pitfalls: "do not import internal/matching/core types directly
// into method signatures beyond what's needed to satisfy core.BreakSink
// in the adapter file"). cmd/matching-batch/main.go (task 12) constructs
// this and passes it in as a core.BreakSink.
type BreakSinkAdapter struct {
	Lifecycle LifecycleService
}

func NewBreakSinkAdapter(lifecycle LifecycleService) *BreakSinkAdapter {
	return &BreakSinkAdapter{Lifecycle: lifecycle}
}

func (a *BreakSinkAdapter) OpenBreak(ctx context.Context, p core.OpenBreakParams) error {
	_, err := a.Lifecycle.OpenBreak(ctx, OpenBreakParams{
		TenantID: p.TenantID, AccountID: p.AccountID,
		TransactionIDs: p.TransactionIDs, BreakType: p.BreakType,
		AmountAtRisk: p.AmountAtRisk, Currency: p.Currency,
	})
	return err
}

var _ core.BreakSink = (*BreakSinkAdapter)(nil)
