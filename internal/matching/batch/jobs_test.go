package batch_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/riverqueue/river"
	"github.com/shopspring/decimal"

	"github.com/koriebruh/Jengine/internal/domain"
	"github.com/koriebruh/Jengine/internal/matching/batch"
	"github.com/koriebruh/Jengine/internal/matching/rules"
)

// stubTransactionRepo returns a fixed, in-memory transaction list per
// account - lets TestMaxPartitionRecordsEnforced sanity-check the 50k
// bounded-working-set cap (plans/task/core/12 Definition of Done)
// without actually inserting 50k+ rows into a real Postgres instance for
// every test run.
type stubTransactionRepo struct {
	domain.TransactionRepository
	byAccount map[uuid.UUID][]domain.Transaction
}

func (s *stubTransactionRepo) ListUnmatched(ctx context.Context, tenantID uuid.UUID, accountID uuid.UUID, from, to time.Time) ([]domain.Transaction, error) {
	return s.byAccount[accountID], nil
}

type noopMatchRuleRepo struct {
	domain.MatchRuleRepository
}

func (noopMatchRuleRepo) ListActive(ctx context.Context, tenantID uuid.UUID, sourceAccountID, targetAccountID uuid.UUID) ([]domain.MatchRule, error) {
	return nil, nil
}

func passthroughTxRunner(ctx context.Context, tenantID uuid.UUID, fn func(context.Context) error) error {
	return fn(ctx)
}

func TestMaxPartitionRecordsEnforced(t *testing.T) {
	tenantID, accountA, accountB := uuid.New(), uuid.New(), uuid.New()

	makeTx := func(accountID uuid.UUID, n int) []domain.Transaction {
		out := make([]domain.Transaction, n)
		for i := range out {
			out[i] = domain.Transaction{
				ID: uuid.New(), TenantID: tenantID, AccountID: accountID,
				BaseAmount: decimal.NewFromInt(int64(i)), Currency: "USD",
				Side: domain.TransactionSideDebit, ExternalRef: "ref",
			}
		}
		return out
	}

	// One over the 50k cap between both sides - must be rejected, not
	// silently processed with unbounded memory.
	over := batch.MaxPartitionRecords/2 + 1
	stub := &stubTransactionRepo{byAccount: map[uuid.UUID][]domain.Transaction{
		accountA: makeTx(accountA, over),
		accountB: makeTx(accountB, over),
	}}

	worker := &batch.PartitionWorker{Deps: batch.WorkerDeps{
		TxRunner:     passthroughTxRunner,
		Transactions: stub,
		MatchRules:   noopMatchRuleRepo{},
		Registry:     rules.DefaultRegistry(),
	}}

	job := &river.Job[batch.PartitionJobArgs]{
		Args: batch.PartitionJobArgs{
			TenantID: tenantID, SourceAccountID: accountA, TargetAccountID: accountB,
			ValueDateBucket: time.Now(),
		},
	}

	err := worker.Work(context.Background(), job)
	if err == nil {
		t.Fatal("expected Work to reject a partition exceeding MaxPartitionRecords, got nil error")
	}
}

func TestMaxPartitionRecordsWithinCapSucceeds(t *testing.T) {
	tenantID, accountA, accountB := uuid.New(), uuid.New(), uuid.New()

	stub := &stubTransactionRepo{byAccount: map[uuid.UUID][]domain.Transaction{
		accountA: {{ID: uuid.New(), TenantID: tenantID, AccountID: accountA, Currency: "USD", Side: domain.TransactionSideDebit, BaseAmount: decimal.NewFromInt(1)}},
		accountB: {{ID: uuid.New(), TenantID: tenantID, AccountID: accountB, Currency: "USD", Side: domain.TransactionSideDebit, BaseAmount: decimal.NewFromInt(1)}},
	}}

	worker := &batch.PartitionWorker{Deps: batch.WorkerDeps{
		TxRunner:     passthroughTxRunner,
		Transactions: stub,
		MatchRules:   noopMatchRuleRepo{}, // no active rules -> Work should just no-op, not error
		Registry:     rules.DefaultRegistry(),
	}}

	job := &river.Job[batch.PartitionJobArgs]{
		Args: batch.PartitionJobArgs{
			TenantID: tenantID, SourceAccountID: accountA, TargetAccountID: accountB,
			ValueDateBucket: time.Now(),
		},
	}

	if err := worker.Work(context.Background(), job); err != nil {
		t.Fatalf("expected Work to succeed (no-op, no active rules) within the cap, got: %v", err)
	}
}
