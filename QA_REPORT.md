# QA Report

Holds only currently-open issues. Fix + re-verify → delete the entry, don't check it off.

## `cmd/webhook-dispatcher` consumes `case.events.default`/`matching.results.default`, not `webhook.outbox`

**Found in:** plans/task/core/21 (webhook system), while building the
dispatcher's Kafka consumer.

**Issue:** plans/task/core/21's own text says "`cmd/webhook-dispatcher`
consumes `webhook.outbox`" - matching plans/task/core/18's topic table,
which describes `webhook.outbox` as the topic for webhook delivery
routing. But tasks 19/20's actual `EmitOutboxEventActivity`/
`reconcile.Reconciler` calls (already committed) write every cataloged
event - `break.created`, `break.assigned`, `match.auto_confirmed`,
`break.sla_warning`, etc. - to `Topic: "case.events.default"`
(reconcile.go) or a caller-supplied topic that's also
`case.events.default` in every wiring done so far (workflow's own
Activities). Nothing publishes to `webhook.outbox` at all.

**Resolution taken:** the dispatcher consumes `case.events.default` and
`matching.results.default` directly (where events actually land) rather
than `webhook.outbox` (which is empty) - the pragmatic fix, since these
are also exactly the topics plans/task/core/21's OWN SSE-gateway
subsection needs to consume for the same event catalog, meaning the
"real" topic split may simply be case-events-serve-both-SSE-and-webhook-
delivery rather than a third topic in between. `webhook.outbox` remains
provisioned (task 18's `create-topics.sh`) but unused.

**Resolution options for a human decision:**
1. Formalize case.events.default/matching.results.default as the ONE
   shared topic family both SSE and webhook delivery consume (what's
   actually built) - retire `webhook.outbox` from the topic list
   entirely, treating plans/task/core/18/21's own text as superseded by
   how tasks 19-21 actually converged.
2. Retrofit tasks 19/20's Activities to ALSO (or instead) write to
   `webhook.outbox` specifically for webhook-catalogable events, keeping
   case.events.default for internal/SSE consumption only - closer to the
   original three-topic design, but requires touching already-committed
   task 19/20 code for a routing concern outside those tasks' own scope.

Not resolved here since it's a genuine topic-design question spanning
three already-built tasks (18, 19, 20), not something to resolve
unilaterally while implementing the dispatcher's consumption side.

## No ingestion-pipeline stage publishes to `normalized.transactions.<shard>` yet

**Found in:** plans/task/core/19 (streaming matching worker), while
building `cmd/matching-stream`.

**Issue:** `cmd/matching-stream` consumes `normalized.transactions.default`
(the Kafka topic plans/task/core/18 provisioned) as its input, expecting
`TransactionEvent` protobuf messages per plans/docs/06-streaming-architecture.md
§7.2. But no stage in the ingestion pipeline (tasks 06-09) actually
publishes onto this topic - the pipeline's canonicalization/persist stage
writes directly to Postgres and stops there. `internal/ingestion/kafka.Producer`
exists (task 06) but nothing in `internal/ingestion` calls it for
per-transaction events; it's only exercised by `internal/ingestion`'s own
outbox relay for a different topic (`ingestion.raw.<shard>`, the raw pre-
normalization stage - see the "two outbox mechanisms" entry above for how
that relates to task 18's `outbox_event`).

**Current workaround:** `cmd/matching-stream -demo=publish-test-event`
publishes synthetic `TransactionEvent` messages for manual verification
(plans/task/core/19's own Definition of Done explicitly calls for this:
"publish a synthetic streaming event, observe a provisional
AUTO_MATCHED_STREAMING result") - this is what was used to verify
`matching-stream`'s consumption side and the batch/streaming
reconciliation end-to-end (see this task's commit message), proving the
CONSUMER half works correctly against the documented schema, independent
of this gap.

**Impact:** `matching-stream` has nothing real to consume in an actual
deployment until this is closed - streaming matching is built and
verified, but dormant against real ingestion traffic.

**Resolution options for a human decision:**
1. Add a publish step to the ingestion pipeline's canonicalization/
   persist stage (task 08/09's own code) that produces a
   `TransactionEvent` onto `normalized.transactions.<shard>` via the SAME
   outbox pattern task 18/19 already established (write to `outbox_event`
   in the same transaction as the canonical `Transaction` insert) -
   the most consistent fix, reusing the existing mechanism rather than a
   third one.
2. Have the persist stage produce directly via `internal/ingestion/kafka.Producer`
   post-commit (simpler, but reintroduces the dual-write risk the outbox
   pattern exists to avoid - not recommended without a clear reason to
   deviate).

Not resolved here since it requires touching already-committed tasks
08/09's own pipeline code for a concern (event publishing) outside task
19's own scope (the streaming matching worker itself, not the producer
side).

## Webhook-receiver connector configs only load at `cmd/coreapi` startup, no live refresh

**Found in:** plans/task/core/18 (streaming ingestion), while wiring the
webhook-receiver connector's HTTP mount into `cmd/coreapi`.

**Issue:** `webhookreceiver.Connector` needs each tenant's webhook
connector config registered (via `.Fetch(ctx, cfg)`) before it can route
an incoming request by `connector_id`. `cmd/coreapi/main.go`'s
`loadWebhookConnectors` does this once, at process startup, via a raw
`SELECT ... FROM connectors WHERE type = 'webhook' AND status = 'ACTIVE'`
query. A webhook connector created (or reactivated) *after* the process
starts is invisible until the next restart - there's no
ConnectorService/API endpoint yet (task 15's service list has none) that
could call `.Fetch()` at connector-creation time instead.

**Impact:** Onboarding a new tenant's webhook integration requires a
`cmd/coreapi` restart today. Not a correctness bug for already-configured
connectors, just an operational gap for adding new ones live.

**Resolution options for a human decision:**
1. Add a `ConnectorService` (Connect-RPC, following the same handler
   pattern as `AccountServiceHandler` etc.) whose `CreateConnector`
   (webhook type) calls `webhookReceiver.Fetch()` immediately after the
   DB insert - closes the gap, but ConnectorService itself isn't
   currently scoped to any existing task.
2. Poll the `connectors` table periodically (e.g. every 30s) from
   `cmd/coreapi` and re-run `loadWebhookConnectors`, diffing against
   already-registered connector IDs - cheap, no new API surface, but a
   real (bounded) staleness window.
3. Accept the restart-required behavior as an operational runbook step
   for MVP (document it, revisit if webhook onboarding frequency makes
   it painful).

Not resolved here since it depends on whether/when a general
Connector-management API surface gets built - a scope decision beyond
this task's own boundary (webhook connector mechanics, HMAC verification,
dedup), not something to invent unilaterally in passing.

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

**Same gap inherited by the streaming worker (`internal/matching/stream`,
plans/task/core/19):** with no `account_group`/scope representation to
narrow by, `stream.Consumer` pools candidates per-TENANT (not per-
account-pair) and loads every ACTIVE rule for the tenant tenant-wide
(`MatchRuleRepository.ListByTenant`, not the account-pair-scoped
`ListActive` the batch worker uses) rather than per account pair.
`core.Match`'s own blocking index still filters the resulting (broader)
candidate set correctly - same "correct but broader than necessary"
tradeoff as the batch workaround above, not a new correctness gap. Fixing
this (option 1 above) would narrow both workarounds at once.

## Two outbox mechanisms with overlapping intent (`ingestion_outbox` vs `outbox_event`)

**Found in:** plans/task/core/18 (streaming ingestion), while building the
general transactional-outbox table.

**Issue:** `plans/task/core/06`/`09` already built `ingestion_outbox`
(`id, tenant_id, topic, key, payload, created_at, sent_at`) + a Go
`OutboxWriter`/`OutboxReader`/`OutboxRelay` poller that marks `sent_at`
once a row is relayed to Kafka - built and working, used by
`cmd/ingestion-gateway`'s real seed flow. `plans/task/core/18` specifies
a *second*, more general `outbox_event` table
(`aggregate_type, aggregate_id, event_type, topic, payload` - no
`sent_at`, no Go poller) meant to be consumed via Debezium's CDC outbox-
event-router SMT instead - the foundation tasks 19/21/22 build on.

Both exist now, side by side, doing conceptually the same job
(transactional outbox → Kafka) via two different mechanisms (Go-poller-
with-sent_at vs. CDC-stream-of-inserts) for two different call sites
(ingestion pipeline vs. everything else). Task 18's own spec doesn't say
to migrate task 06's usage onto the new table - only to build the new
one for its own scope (webhook receiver, kafka-source connector, and
later tasks' event emission).

**Impact:** No functional bug today - each mechanism works correctly for
its own current callers. But two outbox tables/consumption models is
genuine architectural duplication that will confuse the next person
touching either, and `internal/ingestion`'s `OutboxRelay` poller vs.
Debezium CDC are operationally different things to run/monitor.

**Resolution options for a human decision:**
1. Migrate `internal/ingestion`'s outbox writes onto `outbox_event` +
   Debezium, retire `ingestion_outbox`/`OutboxRelay` entirely - one
   mechanism, but a real migration of already-working, tested code.
2. Keep both permanently: `ingestion_outbox` for the ingestion
   pipeline's own simple relay need, `outbox_event` for the general CDC-
   driven case tasks 19/21/22 need. Document the split as intentional
   rather than accidental.
3. Migrate the other direction (retire `outbox_event`, extend
   `ingestion_outbox`'s shape + `OutboxRelay` to cover the general case)
   - keeps one mechanism without adopting Debezium, but Debezium is
   explicitly the design's own chosen mechanism for §7.3's outbox
   pattern, so this fights the design doc rather than following it.

Not resolved here since it's a genuine trade-off between "one mechanism"
and "don't touch already-working tested code" - not something to decide
unilaterally while implementing task 18's own narrower scope.
