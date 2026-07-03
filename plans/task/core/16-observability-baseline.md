# Task 16: Observability Baseline

## Goal
Instrument the application code itself with OpenTelemetry: tracing that spans the full flow (ingestion → normalization → matching → case creation, extending to webhook delivery once task 21 exists), golden-signal metrics per service, and structured logging correlated to trace context. This is the code-level half of observability — the infra half (Prometheus/Tempo/Jaeger/Loki/Grafana running locally) is already covered by core task 02's docker-compose `--profile observability`. This task exists because `plans/docs/12-competitive-differentiation.md` names "opaque internals, hard to debug 'why didn't this match'" as a specific ReconArt weakness Jengine is meant to answer — that only happens if every binary actually emits the traces/metrics/logs described here, not just if the infra to receive them exists.

## Prerequisites
- Task 01 (repo bootstrap and tooling) — this task adds SDK wiring to whatever binaries already exist; it has no hard functional blocker beyond a buildable Go module, but it's most useful once `cmd/coreapi` (task 15) and `cmd/matching-batch` (task 12) exist to instrument. Wire this in as each binary is built rather than treating it as strictly sequential — see task 17 for the same "don't treat MVP task numbering as a strict build-only-in-order rule" point.

## Scope / Deliverables
- `internal/platform/observability/otel.go` — `InitTracerProvider`, `InitMeterProvider`, shutdown/flush helpers, OTLP exporter configuration (env-configured endpoint, defaulting to the local Jaeger/Tempo collector from task 02's compose file).
- `internal/platform/observability/logging.go` — `NewLogger(cfg Config) *slog.Logger`, with a `slog.Handler` wrapper that injects `trace_id`/`span_id` into every log record when a span is active in context.
- `internal/platform/observability/metrics.go` — golden-signal metric definitions (request rate, error rate, duration histograms) plus the business metrics this domain specifically needs (match rate, auto-match rate, break-open count, audit-chain-verification-failure count).
- `internal/platform/observability/middleware.go` — a Connect-RPC interceptor (for `cmd/coreapi`, task 15) and a worker-loop wrapper (for `cmd/matching-batch`, task 12) that automatically start/end spans and record golden-signal metrics per call/job, so individual handlers don't have to hand-instrument every function.
- Wiring changes to `cmd/coreapi/main.go` and `cmd/matching-batch/main.go` (task 15/12 binaries) to call `InitTracerProvider`/`InitMeterProvider`/`NewLogger` at startup and pass the logger/tracer through dependency injection, plus deferred shutdown/flush on process exit.

## Design Reference
- `plans/docs/10-observability-reliability.md` §11.2 — "OpenTelemetry SDK in every service — traces span ingestion→normalization→matching→case creation→webhook delivery, single `trace_id` for 'why didn't this match' debugging." §11.2 also calls for "golden-signals dashboards per service + business-metric dashboards (match rate %, auto-match %, break aging, SLA compliance %) per tenant" — this task produces the metrics those dashboards consume; building the dashboards themselves is out of scope (see Non-Goals).
- `plans/docs/10-observability-reliability.md` §11.1 SLO table — the concrete numbers this instrumentation should make measurable (streaming match p99 < 1s — not measurable yet at MVP since there's no streaming path, but the batch equivalent and API availability numbers are).
- `plans/docs/16-development-workflow.md` §16.2 — local dev's optional `--profile observability` (Jaeger + Prometheus + Grafana) is the receiving end for what this task emits; already built by task 02, not rebuilt here.
- `plans/docs/00-overview-and-architecture.md` §1.3 — "OpenTelemetry → Prometheus + Tempo/Jaeger + Loki + Grafana" as the chosen stack (vendor-neutral, CNCF-standard) — this task targets OTLP export, compatible with any of these collectors without app-code changes if the backend is swapped later.

## Implementation Notes

### Tracer/meter/logger initialization
```go
type Config struct {
    ServiceName    string
    ServiceVersion string
    OTLPEndpoint   string // env-configured, defaults to local collector
    Environment    string // dev|staging|prod
}

func InitTracerProvider(ctx context.Context, cfg Config) (*sdktrace.TracerProvider, error)
func InitMeterProvider(ctx context.Context, cfg Config) (*sdkmetric.MeterProvider, error)
func NewLogger(cfg Config) *slog.Logger
```
Each `cmd/*/main.go` calls these once at startup, sets the global `otel.SetTracerProvider`/`otel.SetMeterProvider`, and passes the concrete `*slog.Logger` through constructor injection (per `plans/docs/16-development-workflow.md` §16.3's "manual constructor injection, no DI framework" convention — this task follows that, doesn't introduce Wire/Fx). Use `log/slog` (Go stdlib) rather than a third-party logging library — zero new dependency, and pairs cleanly with an OTel logs bridge if/when one is added.

### Trace propagation across the flow
The end-to-end trace described in §11.2 requires a `trace_id` to survive from ingestion through to case creation. At MVP (no Kafka yet — task 18/19 are V1), the practical propagation points are:
1. HTTP/Connect-RPC request → span started by the interceptor (`middleware.go`), context carries it through all in-process calls (repositories, task 11's compiler, task 13's `LifecycleService`).
2. `cmd/matching-batch`'s job queue jobs (task 12, River) → start a new root span per job (batch jobs are not directly caused by a single traced HTTP request, so there's no parent trace to continue — this is fine and expected; note it rather than trying to force a fake parent), but propagate that span's context through the whole partition-processing call chain including the `BreakSink.OpenBreak` call into task 13.
3. Once Kafka/streaming exists (V1), trace context propagates via message headers — not this task's concern to build, but the span-per-unit-of-work pattern established here should make that extension straightforward later; don't design something that has to be thrown away.

### Golden-signal + business metrics
- Per Connect-RPC method: request count (by method, status code), error count, duration histogram.
- Per batch job (task 12): job count, duration histogram, records-processed counter, partition-size gauge/histogram (useful for catching the "50k working-set cap not enforced" pitfall called out in task 12).
- Business metrics: `match_auto_matched_total`, `match_suggested_total`, `match_unmatched_total` (all labeled by tenant + rule), `break_opened_total`, `break_resolved_total` (labeled by tenant, root cause), `audit_chain_verification_failures_total` (task 14's verification job increments this on any detected break).
- Expose via OTLP metrics export (or a Prometheus-compatible `/metrics` endpoint if simpler for local scraping — either is acceptable at MVP as long as task 02's optional observability profile can actually receive it; confirm compatibility rather than assuming).

### Structured logging correlation
The `slog.Handler` wrapper should read the active span from context (`trace.SpanContextFromContext`) and attach `trace_id`/`span_id` as structured fields on every log record automatically, so a support engineer can jump from a log line to the matching trace without manual correlation.

## Non-Goals / Guardrails
- No standing up of Prometheus/Tempo/Jaeger/Loki/Grafana infrastructure — that's core task 02's docker-compose `--profile observability`, already built. This task only makes the application emit data those collectors can receive.
- No Grafana dashboard JSON/provisioning — building actual dashboards (even basic ones) is not required for this task's Definition of Done; the metrics existing and being scrapeable is sufficient. Note dashboard-building as a natural follow-up, not blocking.
- No ClickHouse-backed business dashboards (`mv_match_rate_by_rule`, etc.) — that's task 22 (V1), which has its own analytics pipeline; this task's business metrics are OTel/Prometheus-shaped operational metrics, not the ClickHouse materialized-view reporting layer.
- No SLA breach alerting/escalation logic — that depends on task 20's Temporal-based SLA timers (V1); this task can emit a `break_sla_due_soon` gauge if trivial, but building alerting rules is out of scope.
- No distributed trace propagation across Kafka message headers — no Kafka exists yet at MVP (tasks 18/19 are V1); don't build speculative infrastructure for a transport that doesn't exist yet.
- Do not introduce a third-party structured-logging library (zerolog/zap) when stdlib `log/slog` is sufficient and dependency-light — consistent with the project's general "no heavyweight framework until scale demands it" convention (`plans/docs/16-development-workflow.md` §16.3).

## Definition of Done
- Unit tests confirming the `slog.Handler` wrapper correctly injects `trace_id`/`span_id` when a span is active, and omits them cleanly when it isn't (no panic on missing span).
- Unit tests for the Connect-RPC interceptor and batch-job wrapper confirming spans are started/ended and golden-signal metrics are recorded for both success and error paths.
- Integration test: run `cmd/coreapi` locally with a real OTLP collector (or an in-memory/test exporter) and assert a request produces a span with the expected name/attributes and a log line with matching `trace_id`.
- `go test -race ./internal/platform/observability/...` passes.
- Manual verification: bring up the local dev stack with `docker compose --profile observability up` (task 02), exercise a request against `cmd/coreapi` and a job against `cmd/matching-batch`, and confirm traces appear in the local Jaeger/Tempo UI and metrics appear in Prometheus/Grafana.
- Completion is tests passing; exploratory QA issues go in root-level `QA_REPORT.md`, open items only, deleted when fixed.

## Common Pitfalls
- Adding OTel SDK calls scattered ad hoc inside individual handlers instead of centralizing span/metric start-stop in the interceptor/middleware layer — leads to inconsistent coverage (some endpoints traced, some not) and duplicated boilerplate; the interceptor pattern in `middleware.go` exists specifically to make instrumentation automatic and consistent.
- Forgetting to call the tracer/meter provider's shutdown/flush on process exit — spans/metrics generated right before a graceful shutdown get silently dropped, which is exactly the kind of gap that makes "why didn't this match" debugging fail on the cases that matter most (a batch job that was mid-flight during a deploy).
- Building a fully custom dashboard suite or alerting rules when the task only requires the application to emit correctly-shaped, receivable telemetry — scope discipline matters here since dashboard-building can expand indefinitely; the infra to receive telemetry already exists (task 02), this task's job ends at correct emission.
- Trying to force a single trace_id across the batch worker's job-queue-triggered runs back to the original ingestion request that produced the underlying transactions — at MVP there often isn't a live parent trace to attach to (batch runs are scheduled/triggered independently); starting a fresh root span per job is the correct MVP behavior, not a shortcut to fix.
