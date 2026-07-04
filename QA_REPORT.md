# QA Report

Holds only currently-open issues. Fix + re-verify → delete the entry, don't check it off.

## `account_group` rule-scope taxonomy was never given a schema/domain representation

**Found in:** plans/task/core/12 (matching batch worker), while implementing `EnumeratePartitions`.

**Issue:** `plans/docs/04-matching-engine.md` §5.1's rule DSL example specifies
`scope: { source: { account_group: "bank_accounts" }, target: { account_group: "gl_cash_accounts" } }`,
implying accounts are taggable into named groups that a rule's scope
references to determine which account pairs it applies to. This concept
was never given:
- a schema column/table in `plans/task/core/03`'s migrations (no
  `account_group` column on `accounts`, no separate grouping table), or
- a domain type in `plans/task/core/05` (`domain.Account` has no group
  field).

`plans/task/core/11`'s `RuleSpec` does parse `scope.source`/
`scope.target.account_group` into `ScopeSpec` (since it's just parsing
the YAML shape faithfully), but `Compile()` has nowhere to put it -
`core.CompiledRule` (`plans/task/core/10`) has no `Scope` field, so the
parsed value is silently unused past parsing.

**Current workaround (`internal/matching/batch/partition.go`):**
`EnumeratePartitions` pairs every distinct account with `UNMATCHED`
transactions against every other distinct account in the same tenant and
day that also has `UNMATCHED` transactions - i.e., an unordered
cross-product bounded by a tenant's account count (not transaction
volume), rather than the rule-scope-filtered account pairing the design
doc describes. This is correct (no false negatives - every pair that
*should* be considered is included) but broader than necessary (some
pairs that a real account_group taxonomy would have excluded, e.g.
bank-vs-bank, still get a partition and a `Match` run that will simply
find no candidates most of the time).

**Impact:** Extra partitions/`Match` calls that terminate quickly with no
candidates (blocking still applies within each, so this isn't an O(N×M)
regression) - a performance/precision gap, not a correctness one, at
current MVP account-count scale.

**Resolution options for a human decision:**
1. Add an `account_group` column (or a many-to-many tagging table, if an
   account can belong to multiple groups) to `accounts` in a new
   migration, a `Scope` field to `core.CompiledRule`, and have
   `EnumeratePartitions`/`Compile` use it to filter pairs - closes the
   gap as originally designed.
2. Accept the current unordered-cross-product behavior as the MVP
   answer permanently (revisit only if/when account counts per tenant
   grow large enough for the extra partitions to matter).

Not resolved here since it touches schema (task 03) and `core.CompiledRule`
(task 10), both already-committed tasks - a decision on which of the two
options above (or another) belongs to whoever owns that trade-off, not a
unilateral schema change made in passing while building the batch worker.

**Related consequence, fixed in `internal/matching/batch/jobs.go`:**
because partitions are unordered but `domain.MatchRuleRepository.ListActive`
matches `source_account_id`/`target_account_id` directionally as stored,
`loadCompiledRules` now queries both orderings and merges by priority -
otherwise a rule stored `(accountA -> accountB)` would silently never be
found for a partition enumerated as `(accountB, accountA)`. This doubles
the rule-lookup query count per partition (cheap, indexed lookups) and
is itself a symptom of the same missing-taxonomy gap, not a separate
issue.
