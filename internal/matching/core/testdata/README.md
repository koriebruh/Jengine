# Golden-dataset fixture convention

Each subdirectory here is one test case for `golden_test.go`:

```
testdata/<case_name>/
  source.json    # []core.MatchableRecord - source-side records
  target.json    # []core.MatchableRecord - target-side records
  rules.yaml     # []core.CompiledRule, rendered directly via CompiledRule's
                 # own yaml tags - NOT the tenant-facing DSL shape from
                 # plans/docs/02-data-ingestion.md/04-matching-engine.md
                 # §5.1 that plans/task/core/11's real compiler parses.
                 # core.Match never parses YAML itself (plans/task/core/10
                 # Non-Goals) - these fixtures use the compiled form both
                 # tasks agree on directly.
  expected.json  # core.MatchOutcome - the outcome core.Match must produce
                 # ("unmatched" is a single []uuid.UUID list of whatever's
                 # left over from source+target combined, not separate
                 # unmatched_source/unmatched_target lists)
```

`golden_test.go` walks every subdirectory here, loads the four files, calls
`core.Match(ctx, source, target, rules, registry)` (using a small test-local
`ScoringRegistry` standing in for plans/task/core/11's real similarity
functions), and compares the result against `expected.json` by ID-set
membership (not byte-for-byte, since map/slice iteration order for
`FieldScores` isn't guaranteed stable).

**This is a living convention, not a one-time seed** (plans/task/core/17
Common Pitfalls) - every new blocking/scoring behavior plans/task/core/10+
adds should get a new case here. A scoring/blocking change that breaks an
existing fixture must update `expected.json` as a reviewed, visible diff -
never silently.

Cases as of plans/task/core/10:
- `case_empty_placeholder/` - zero records, zero rules, empty outcome;
  proves the load-run-diff mechanism itself works.
- `case_one_to_one_auto_match/` - exact currency/date/amount/reference ->
  score 1.0, above auto_match_threshold.
- `case_one_to_one_suggested/` - matching currency/date/amount but a
  different reference -> score lands between suggest_threshold and
  auto_match_threshold.
- `case_unmatched_no_candidate/` - source and target never share a
  blocking bucket (different currency) -> both end up unmatched.
- `case_priority_chaining/` - two rules; a stricter higher-priority rule
  (blocked on exact reference) catches one pair and leaves the other
  untouched (different references means it's never even a candidate under
  that rule); a looser lower-priority rule then catches the residual pair.
