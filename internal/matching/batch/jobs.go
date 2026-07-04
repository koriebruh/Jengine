package batch

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/riverqueue/river"

	"github.com/koriebruh/Jengine/internal/domain"
	"github.com/koriebruh/Jengine/internal/matching/core"
	"github.com/koriebruh/Jengine/internal/matching/rules"
)

// MaxPartitionRecords bounds the per-partition working set
// (plans/docs/04-matching-engine.md §5.2: "max 50k records") - loading
// beyond this defeats the bounded-memory design point this worker exists
// to uphold (plans/task/core/12 Common Pitfalls: "loading an entire
// tenant's unmatched transactions into memory... defeats the
// scalability design point").
const MaxPartitionRecords = 50_000

// ruleCacheTTL mirrors mapping.MappingEngine's short-TTL spec cache
// pattern (plans/task/core/08), reused here for compiled MatchRules
// (plans/task/core/12 Implementation Notes: "cached with short TTL to
// avoid recompiling every partition").
const ruleCacheTTL = 30 * time.Second

// LoadWindowMarginDays widens each partition's transaction query beyond
// its exact day bucket - see PartitionKey.LoadWindow's doc comment for
// why a strict single-day load would silently defeat a rule's
// date_window tolerance near a bucket boundary.
const LoadWindowMarginDays = 3

// PartitionJobArgs is the River job payload for one partition's match
// run - a flattened PartitionKey (River requires JSON-serializable args).
type PartitionJobArgs struct {
	TenantID        uuid.UUID `json:"tenant_id"`
	SourceAccountID uuid.UUID `json:"source_account_id"`
	TargetAccountID uuid.UUID `json:"target_account_id"`
	ValueDateBucket time.Time `json:"value_date_bucket"`
}

func (PartitionJobArgs) Kind() string { return "matching_partition" }

func (a PartitionJobArgs) partitionKey() PartitionKey {
	return PartitionKey(a)
}

// TxRunner wraps fn in a transaction scoped to tenantID - same shape and
// rationale as every other DB-touching component in this pipeline (see
// mapping.TxRunner, connector/csvupload.TxRunner,
// connector/sftp.TxRunner): satisfied by a closure around postgres.WithTx
// in production, a pass-through in tests.
type TxRunner func(ctx context.Context, tenantID uuid.UUID, fn func(ctx context.Context) error) error

// WorkerDeps are PartitionWorker's dependencies, all interfaces so
// cmd/matching-batch/main.go wires in the concrete implementations
// (including the real BreakSink from plans/task/core/13, once it
// exists) - internal/matching/batch itself never imports internal/cases
// or internal/storage/postgres directly, matching task 10/11's own
// layering discipline (plans/docs/16-development-workflow.md §16.1).
type WorkerDeps struct {
	TxRunner     TxRunner
	Transactions domain.TransactionRepository
	MatchResults domain.MatchResultRepository
	MatchRules   domain.MatchRuleRepository
	Registry     core.ScoringRegistry
	BreakSink    core.BreakSink
}

// PartitionWorker implements river.Worker[PartitionJobArgs] - the actual
// per-partition work plans/docs/04-matching-engine.md §5.2 describes:
// load, compile rules, match, write back.
type PartitionWorker struct {
	river.WorkerDefaults[PartitionJobArgs]
	Deps WorkerDeps

	cacheMu sync.RWMutex
	cache   map[string]cachedRules
}

type cachedRules struct {
	rules     []core.CompiledRule
	expiresAt time.Time
}

func (w *PartitionWorker) Work(ctx context.Context, job *river.Job[PartitionJobArgs]) error {
	args := job.Args
	start, end := args.partitionKey().LoadWindow(LoadWindowMarginDays)

	var sourceTx, targetTx []domain.Transaction
	var compiledRules []core.CompiledRule

	err := w.Deps.TxRunner(ctx, args.TenantID, func(ctx context.Context) error {
		var err error
		sourceTx, err = w.Deps.Transactions.ListUnmatched(ctx, args.TenantID, args.SourceAccountID, start, end)
		if err != nil {
			return fmt.Errorf("load source transactions: %w", err)
		}
		targetTx, err = w.Deps.Transactions.ListUnmatched(ctx, args.TenantID, args.TargetAccountID, start, end)
		if err != nil {
			return fmt.Errorf("load target transactions: %w", err)
		}

		compiledRules, err = w.loadCompiledRules(ctx, args.TenantID, args.SourceAccountID, args.TargetAccountID)
		return err
	})
	if err != nil {
		return fmt.Errorf("batch: partition load: %w", err)
	}

	totalRecords := len(sourceTx) + len(targetTx)
	if totalRecords > MaxPartitionRecords {
		// Reject rather than silently load everything - a partition this
		// large must be split further (a narrower LoadWindow/date bucket)
		// by whoever enqueues jobs, not quietly handled here with
		// unbounded memory (plans/task/core/12 Common Pitfalls).
		return fmt.Errorf("batch: partition %s/%s/%s has %d records, exceeds bounded working set cap %d - split by a narrower date range",
			args.TenantID, args.SourceAccountID, args.TargetAccountID, totalRecords, MaxPartitionRecords)
	}

	if len(compiledRules) == 0 {
		return nil // no active rules for this account pair - nothing to do
	}

	source := make([]core.MatchableRecord, len(sourceTx))
	txByID := make(map[uuid.UUID]domain.Transaction, len(sourceTx)+len(targetTx))
	for i, t := range sourceTx {
		source[i] = toMatchableRecord(t)
		txByID[t.ID] = t
	}
	target := make([]core.MatchableRecord, len(targetTx))
	for i, t := range targetTx {
		target[i] = toMatchableRecord(t)
		txByID[t.ID] = t
	}

	outcome, err := core.Match(ctx, source, target, compiledRules, w.Deps.Registry)
	if err != nil {
		return fmt.Errorf("batch: match: %w", err)
	}

	return WriteResults(ctx, w.Deps, args.TenantID, outcome, txByID)
}

func (w *PartitionWorker) loadCompiledRules(ctx context.Context, tenantID, sourceAccountID, targetAccountID uuid.UUID) ([]core.CompiledRule, error) {
	cacheKey := fmt.Sprintf("%s|%s|%s", tenantID, sourceAccountID, targetAccountID)

	w.cacheMu.RLock()
	cached, ok := w.cache[cacheKey]
	w.cacheMu.RUnlock()
	if ok && time.Now().Before(cached.expiresAt) {
		return cached.rules, nil
	}

	// EnumeratePartitions produces unordered account pairs (see
	// partition.go's doc comment - source/target are an arbitrary
	// assignment, not the directional bank-vs-GL distinction a real
	// account_group taxonomy would give, per the QA_REPORT.md gap), but
	// domain.MatchRuleRepository.ListActive matches source/target
	// directionally as stored. Query both orderings and merge - a rule
	// stored (accountA -> accountB) must still be found for a partition
	// enumerated as (accountB, accountA).
	matchRules, err := w.Deps.MatchRules.ListActive(ctx, tenantID, sourceAccountID, targetAccountID)
	if err != nil {
		return nil, fmt.Errorf("list active match rules: %w", err)
	}
	reverseRules, err := w.Deps.MatchRules.ListActive(ctx, tenantID, targetAccountID, sourceAccountID)
	if err != nil {
		return nil, fmt.Errorf("list active match rules (reverse direction): %w", err)
	}
	matchRules = append(matchRules, reverseRules...)
	sort.Slice(matchRules, func(i, j int) bool { return matchRules[i].Priority < matchRules[j].Priority })

	compiled := make([]core.CompiledRule, 0, len(matchRules))
	for _, mr := range matchRules {
		spec, err := rules.ParseJSON(mr.RuleSpec)
		if err != nil {
			return nil, fmt.Errorf("parse rule %s (%s): %w", mr.ID, mr.Name, err)
		}
		cr, err := rules.Compile(spec, w.Deps.Registry)
		if err != nil {
			return nil, fmt.Errorf("compile rule %s (%s): %w", mr.ID, mr.Name, err)
		}
		cr.ID = mr.ID
		cr.TenantID = mr.TenantID
		compiled = append(compiled, cr)
	}

	w.cacheMu.Lock()
	if w.cache == nil {
		w.cache = make(map[string]cachedRules)
	}
	w.cache[cacheKey] = cachedRules{rules: compiled, expiresAt: time.Now().Add(ruleCacheTTL)}
	w.cacheMu.Unlock()

	return compiled, nil
}

func toMatchableRecord(t domain.Transaction) core.MatchableRecord {
	return core.MatchableRecord{
		ID: t.ID, TenantID: t.TenantID, AccountID: t.AccountID,
		ValueDate: t.ValueDate, BaseAmount: t.BaseAmount, Currency: t.Currency,
		Reference: t.ExternalRef, CounterpartyRef: t.CounterpartyRef, Side: string(t.Side),
	}
}

var _ river.Worker[PartitionJobArgs] = (*PartitionWorker)(nil)
