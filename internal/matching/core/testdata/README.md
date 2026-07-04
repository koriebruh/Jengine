# Golden-dataset fixture convention

Each subdirectory here is one test case for `golden_test.go`:

```
testdata/<case_name>/
  source.json    # []core.MatchableRecord - source-side records
  target.json    # []core.MatchableRecord - target-side records
  rules.yaml     # raw rule spec (plans/task/core/11's RuleSpec compiler
                 # consumes this once it exists; core.Match currently
                 # ignores it - see the placeholder note in
                 # matching/core/types.go)
  expected.json  # core.MatchOutcome - the outcome core.Match must produce
```

`golden_test.go` walks every subdirectory here, loads the four files, calls
`core.Match(source, target, rules)`, and fails if the result doesn't
exactly match `expected.json`.

**This is a living convention, not a one-time seed** (plans/task/core/17
Common Pitfalls) - every new blocking/scoring behavior plans/task/core/10+
adds should get a new case here. A scoring/blocking change that breaks an
existing fixture must update `expected.json` as a reviewed, visible diff -
never silently.

`case_empty_placeholder/` is the one fixture that exists as of
plans/task/core/17 and 01 - zero source records, zero target records,
empty expected outcome. It proves the load-run-diff mechanism works
before plans/task/core/10 replaces `core.Match`'s placeholder body with
the real matching algorithm and populates real, non-trivial fixtures.
