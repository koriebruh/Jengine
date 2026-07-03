> Part of the Jengine Reconciliation Engine design doc set. Index: [README.md](README.md) · Prev: [15-end-to-end-flows.md](15-end-to-end-flows.md)

# 16 — Repo Structure, Local Dev & Testing Strategy

This doc exists so an implementer doesn't have to guess directory layout, local environment setup, or how correctness gets verified — gaps that otherwise cause inconsistent structure across separate build sessions.

## 16.1 Repository Layout

```
jengine/
├── cmd/                        # one main.go per deployable binary (see 00-overview-and-architecture.md §1.1/§1.2)
│   ├── coreapi/                # modular monolith: tenancy, rules, cases, audit, notify
│   ├── ingestion-gateway/
│   ├── matching-batch/
│   ├── matching-stream/
│   ├── webhook-dispatcher/
│   └── api-gateway/            # public BFF / Connect-RPC edge (can stay colocated with coreapi at MVP)
├── internal/
│   ├── tenancy/                # TenantContext, shard/isolation-tier routing (see 01-multi-tenancy.md)
│   ├── rules/                  # rule config CRUD, versioning, approval-gate wiring (see 04, 05)
│   ├── cases/                  # Break/Case lifecycle, Temporal workflow definitions (see 05-case-management.md)
│   ├── audit/                  # AuditEvent writer, hash-chaining, WORM archival trigger (see 09-security-compliance.md)
│   ├── notify/                 # webhook dispatch, event catalog (see 07-api-extensibility.md §8.2)
│   ├── ingestion/
│   │   ├── connector/          # SourceConnector interface + registry (see 02-data-ingestion.md §3.1)
│   │   ├── parsers/            # mt940/, bai2/, iso20022/, csv/, api/
│   │   └── mapping/            # field-mapping DSL engine (see 02-data-ingestion.md §3.2)
│   ├── matching/
│   │   ├── core/                # engine.go — shared blocking-key + scoring, used by BOTH batch and streaming (see 04-matching-engine.md §5.2)
│   │   ├── rules/                # dsl.go — rule DSL parser/compiler (see 04-matching-engine.md §5.1)
│   │   └── similarity/           # jaro-winkler, levenshtein implementations (see 04-matching-engine.md §5.3)
│   ├── storage/
│   │   ├── postgres/            # repositories, migration runner
│   │   └── clickhouse/          # analytics query layer (see 08-storage-architecture.md §9.2)
│   └── platform/
│       ├── config/              # env/config loading
│       ├── observability/       # OpenTelemetry setup (see 10-observability-reliability.md §11.2)
│       └── authz/                # RBAC/ABAC, OPA client (see 09-security-compliance.md §10.3)
├── proto/jengine/v1/*.proto      # source of truth for API + Kafka event contracts (see 06, 07)
├── migrations/*.sql              # expand-contract only (see 10-observability-reliability.md §11.5)
├── web/                          # Next.js frontend (see 14-dashboard-frontend.md)
├── deploy/
│   ├── helm/
│   └── docker-compose.dev.yml
├── Makefile
└── go.mod
```

**Module-boundary rule**: packages under `internal/` are only importable within this module, and each subpackage should only be imported by `cmd/*` entrypoints that need it — e.g. `matching-batch` and `matching-stream` both import `internal/matching/core`, but neither imports `internal/cases` directly (they only ever create a `Break` row through a well-defined interface/event, never by reaching into the cases package's internals). This is what keeps the "modular monolith → future microservice extraction" path ([00](00-overview-and-architecture.md) §1.1) actually viable later.

## 16.2 Local Development Environment

`deploy/docker-compose.dev.yml` brings up the full local stack (deliberately simplified vs. production — no Citus/multi-node, no Kafka Connect cluster):

| Service | Local substitute | Notes |
|---|---|---|
| OLTP | Plain single-node PostgreSQL | Citus sharding not needed locally; RLS policies still apply so tenant-isolation bugs surface in dev, not just in prod. |
| Message bus | Single-node Redpanda | Kafka-API-compatible, no ZooKeeper — fast to boot. |
| Cache | Redis (single instance) | |
| Object storage | MinIO | S3-compatible; used for statement files + WORM-archive-shaped writes (Object Lock behavior mocked/skipped locally). |
| Workflow engine | Temporal (`temporalio/auto-setup` image + local UI) | Needed as soon as `internal/cases` work starts — not needed for MVP's simple-state-machine phase (see [11](11-scalability-roadmap.md) Phase 0). |
| Observability (optional profile) | Jaeger + Prometheus + Grafana | Only started with `docker compose --profile observability up`, not by default — keeps the default local stack lightweight. |

`make dev-up` / `make dev-down` wrap `docker compose` for this file. `make migrate` runs pending SQL migrations against the local Postgres. `make seed` loads a small fixture tenant + sample MT940 file so a new contributor can exercise Flow 15.1 within minutes of cloning.

## 16.3 Configuration & Secrets Convention

- 12-factor: all config via environment variables, loaded once at boot into a typed Go config struct per binary (validated at startup — fail fast on missing/invalid required config, not on first use).
- No heavyweight config framework needed at this scale — a small hand-written loader (or `envconfig`-style struct tags) is enough; revisit only if config surface grows large enough to justify more tooling.
- Tenant-level secrets (connector credentials, per-tenant KMS key references) are **never** stored inline in `ConnectorConfig`/tenant JSONB — always a Vault path reference, resolved at use-time by the service holding Vault access. Local dev uses a fake/dev Vault (or plain env-var secrets behind a feature flag) to keep the same code path exercised.
- Dependency wiring is manual constructor injection in each `cmd/*/main.go` (no DI framework like Wire/Fx at this scale — the number of services and their dependency graphs are small enough that manual wiring stays readable; reconsider only if a `main.go` wiring block grows unmanageable).

## 16.4 Testing Strategy

| Layer | Approach |
|---|---|
| Unit tests | Standard Go table-driven tests, colocated `_test.go` files per package. |
| Matching engine correctness | **Golden-dataset tests**: a fixture set of (source transactions, target transactions, expected match outcomes) checked into `internal/matching/core/testdata/`, run on every change to blocking/scoring logic to catch silent regressions — this is the most correctness-critical test suite in the whole codebase, since a scoring regression could silently misreconcile production data. |
| Aggregation solver | Property-based tests confirming the many-to-many subset-sum solver ([04](04-matching-engine.md) §5.2) never exceeds `max_group_size`, never double-allocates a transaction across two groups, and always terminates within the capped search space. |
| Connectors | Conformance test suite per connector (via the Connector SDK test harness, [07](07-api-extensibility.md) §8.3) run against real (anonymized) sample files per format — MT940/BAI2/ISO20022 fixtures. |
| Integration tests | `testcontainers-go` spinning up real Postgres + Redpanda + Redis for pipeline-level tests (ingest → normalize → match against a real DB) — financial-correctness code should be tested against real infra behavior, not mocked stand-ins, given how easily a mock/prod divergence hides real bugs in this domain. |
| Rule changes in production | The backtesting sandbox ([04](04-matching-engine.md) §5.4, [15](15-end-to-end-flows.md) §15.3) *is* the pre-production test for a rule change — it must run against real historical data before any rule reaches `ACTIVE`. |

## 16.5 CI Pipeline (suggested stages, in order)

1. `go vet` + `golangci-lint` (includes a custom or grep-based check that every repository query includes an explicit `tenant_id` argument — see [01](01-multi-tenancy.md) §2.2).
2. `go test -race ./...`
3. `buf breaking` against the last-published proto schema (rejects incompatible API/event contract changes, [06](06-streaming-architecture.md) §7.2).
4. Migration lint — reject any migration that isn't expand-contract-safe (e.g. a script flags `DROP COLUMN`/`ALTER COLUMN ... NOT NULL` without a corresponding prior "deprecate" migration, per [10](10-observability-reliability.md) §11.5).
5. Build all `cmd/*` binaries (fails fast on any compile error across the whole module set).
6. On merge to main: build/push container images, ArgoCD syncs to the target environment ([00](00-overview-and-architecture.md) §1.3 deployment stack).

---
Back to: [README.md](README.md)
