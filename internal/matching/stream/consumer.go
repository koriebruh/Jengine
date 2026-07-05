package stream

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"

	"github.com/koriebruh/Jengine/internal/domain"
	"github.com/koriebruh/Jengine/internal/matching/core"
	"github.com/koriebruh/Jengine/internal/matching/rules"
)

// TxRunner wraps fn in a transaction scoped to tenantID - same shape as
// internal/matching/batch.TxRunner and every other package in this
// codebase needing ambient-tx DB access.
type TxRunner func(ctx context.Context, tenantID uuid.UUID, fn func(ctx context.Context) error) error

// WorkerDeps are Consumer's dependencies - deliberately the same shape
// of repository interfaces plans/task/core/12's batch.WorkerDeps uses
// (domain.TransactionRepository/MatchResultRepository/MatchRuleRepository,
// core.ScoringRegistry), so the two workers stay structurally similar
// even though they run on different triggers (Kafka event vs job queue).
type WorkerDeps struct {
	TxRunner     TxRunner
	Transactions domain.TransactionRepository
	MatchResults domain.MatchResultRepository
	MatchRules   domain.MatchRuleRepository
	Registry     core.ScoringRegistry
	Pool         CandidatePool
}

const ruleCacheTTL = 30 * time.Second

type cachedRules struct {
	rules     []core.CompiledRule
	expiresAt time.Time
}

// Consumer processes one incoming transaction event at a time via
// Process. Concurrency/serialization is the CALLER's job (plans/task/
// core/19 Implementation Notes: "serialize processing per (tenant_id,
// account_id) pair... do not rely on a single global mutex") - e.g.
// cmd/matching-stream hashes (tenant_id, account_id) to one of N worker
// goroutines/channels, each calling Process sequentially. Consumer
// itself only guards its own rule cache, not per-tenant processing
// order.
//
// Pooling is tenant-scoped (poolKey = tenantID.String()), not per-
// account-pair, mirroring plans/task/core/12's own documented
// account_group-taxonomy-gap workaround (QA_REPORT.md): there's no
// schema representation yet of which accounts a rule's scope actually
// pairs together, so there's no correct narrower key to partition by.
// internal/matching/core.Match's own blocking index still filters the
// resulting (broader) candidate set correctly - this only costs some
// extra Redis memory/candidates-scored, not a correctness gap.
type Consumer struct {
	Deps WorkerDeps

	cacheMu sync.RWMutex
	cache   map[uuid.UUID]cachedRules // keyed by tenantID
}

// Process matches txn against the tenant's Redis candidate pool using
// only rules whose execution.mode includes "streaming"
// (core.CompiledRule.SupportsStreaming), then adds txn to the pool for
// future lookups. A match clearing a rule's auto_match threshold is
// written as MatchResultStatusAutoMatchedStreaming - PROVISIONAL, never
// final; the batch/streaming reconciliation job
// (internal/matching/reconcile) is what promotes or disputes it later.
func (c *Consumer) Process(ctx context.Context, tenantID uuid.UUID, txn domain.Transaction) error {
	compiledRules, err := c.loadStreamingRules(ctx, tenantID)
	if err != nil {
		return fmt.Errorf("stream: load rules: %w", err)
	}

	poolKey := tenantID.String()
	candidates, err := c.Deps.Pool.Query(ctx, tenantID, poolKey)
	if err != nil {
		return fmt.Errorf("stream: query candidate pool: %w", err)
	}

	incoming := toMatchableRecord(txn)

	if len(compiledRules) > 0 && len(candidates) > 0 {
		outcome, err := core.Match(ctx, []core.MatchableRecord{incoming}, candidates, compiledRules, c.Deps.Registry)
		if err != nil {
			return fmt.Errorf("stream: match: %w", err)
		}
		if err := c.writeOutcome(ctx, tenantID, outcome, incoming, candidates); err != nil {
			return err
		}
	}

	if err := c.Deps.Pool.Add(ctx, tenantID, poolKey, incoming); err != nil {
		return fmt.Errorf("stream: add to candidate pool: %w", err)
	}
	return nil
}

func (c *Consumer) writeOutcome(ctx context.Context, tenantID uuid.UUID, outcome core.MatchOutcome, incoming core.MatchableRecord, candidates []core.MatchableRecord) error {
	if len(outcome.AutoMatched) == 0 {
		return nil
	}

	byID := map[uuid.UUID]core.MatchableRecord{incoming.ID: incoming}
	for _, cand := range candidates {
		byID[cand.ID] = cand
	}

	return c.Deps.TxRunner(ctx, tenantID, func(ctx context.Context) error {
		for _, cand := range outcome.AutoMatched {
			if err := c.writeMatchResult(ctx, tenantID, cand); err != nil {
				return err
			}
			// Remove matched candidates from the pool so they aren't
			// offered again as someone else's candidate - the
			// transaction itself isn't marked MATCHED yet (that's
			// still the batch pass's authority once it reconciles;
			// see plans/task/core/19's provisional-status framing),
			// only its pool membership is retired.
			for _, id := range append(append([]uuid.UUID{}, cand.SourceIDs...), cand.TargetIDs...) {
				if id == incoming.ID {
					continue // not yet in the pool, nothing to remove
				}
				if err := c.Deps.Pool.Remove(ctx, tenantID, tenantID.String(), id); err != nil {
					return fmt.Errorf("remove matched candidate from pool: %w", err)
				}
			}
		}
		return nil
	})
}

func (c *Consumer) writeMatchResult(ctx context.Context, tenantID uuid.UUID, cand core.ScoredCandidate) error {
	cardinality := domain.MatchCardinalityOneToOne
	switch {
	case len(cand.SourceIDs) > 1:
		cardinality = domain.MatchCardinalityManyToOne
	case len(cand.TargetIDs) > 1:
		cardinality = domain.MatchCardinalityOneToMany
	}

	ruleID := cand.RuleID
	result := domain.MatchResult{
		RuleID:          &ruleID,
		MatchType:       cardinality,
		ConfidenceScore: decimal.NewFromFloat(cand.Score),
		Status:          domain.MatchResultStatusAutoMatchedStreaming,
		MatchedAt:       time.Now(),
	}

	lines := make([]domain.MatchResultLine, 0, len(cand.SourceIDs)+len(cand.TargetIDs))
	for _, id := range cand.SourceIDs {
		lines = append(lines, domain.MatchResultLine{TransactionID: id, TenantID: tenantID, Side: domain.MatchResultLineSideSource})
	}
	for _, id := range cand.TargetIDs {
		lines = append(lines, domain.MatchResultLine{TransactionID: id, TenantID: tenantID, Side: domain.MatchResultLineSideTarget})
	}

	if _, err := c.Deps.MatchResults.Create(ctx, tenantID, result, lines); err != nil {
		return fmt.Errorf("write streaming match result for rule %s: %w", cand.RuleID, err)
	}
	return nil
}

// loadStreamingRules loads every ACTIVE rule for tenantID (tenant-wide,
// not account-pair-scoped - see Consumer's own doc comment), compiles
// each, and keeps only those whose execution.mode includes "streaming"
// (plans/task/core/19: "Only rules with execution.mode including
// streaming run in this path"). Cached briefly per tenant, same
// rationale as batch.PartitionWorker's own rule cache: rules change
// rarely, but Process runs per-event and can't afford a DB round trip
// each time.
func (c *Consumer) loadStreamingRules(ctx context.Context, tenantID uuid.UUID) ([]core.CompiledRule, error) {
	c.cacheMu.RLock()
	cached, ok := c.cache[tenantID]
	c.cacheMu.RUnlock()
	if ok && time.Now().Before(cached.expiresAt) {
		return cached.rules, nil
	}

	var matchRules []domain.MatchRule
	err := c.Deps.TxRunner(ctx, tenantID, func(ctx context.Context) error {
		var err error
		matchRules, err = c.Deps.MatchRules.ListByTenant(ctx, tenantID, domain.MatchRuleStatusActive)
		return err
	})
	if err != nil {
		return nil, fmt.Errorf("list active match rules: %w", err)
	}
	sort.Slice(matchRules, func(i, j int) bool { return matchRules[i].Priority < matchRules[j].Priority })

	compiled := make([]core.CompiledRule, 0, len(matchRules))
	for _, mr := range matchRules {
		spec, err := rules.ParseJSON(mr.RuleSpec)
		if err != nil {
			return nil, fmt.Errorf("parse rule %s (%s): %w", mr.ID, mr.Name, err)
		}
		cr, err := rules.Compile(spec, c.Deps.Registry)
		if err != nil {
			return nil, fmt.Errorf("compile rule %s (%s): %w", mr.ID, mr.Name, err)
		}
		if !cr.SupportsStreaming() {
			continue
		}
		cr.ID = mr.ID
		cr.TenantID = mr.TenantID
		compiled = append(compiled, cr)
	}

	c.cacheMu.Lock()
	if c.cache == nil {
		c.cache = make(map[uuid.UUID]cachedRules)
	}
	c.cache[tenantID] = cachedRules{rules: compiled, expiresAt: time.Now().Add(ruleCacheTTL)}
	c.cacheMu.Unlock()

	return compiled, nil
}

func toMatchableRecord(t domain.Transaction) core.MatchableRecord {
	return core.MatchableRecord{
		ID: t.ID, TenantID: t.TenantID, AccountID: t.AccountID,
		ValueDate: t.ValueDate, BaseAmount: t.BaseAmount, Currency: t.Currency,
		Reference: t.ExternalRef, CounterpartyRef: t.CounterpartyRef, Side: string(t.Side),
	}
}
