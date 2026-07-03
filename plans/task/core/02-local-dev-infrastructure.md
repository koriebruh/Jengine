# Task 02: Local Dev Infrastructure

## Goal
Stand up the local development infrastructure stack (Postgres, Redpanda, Redis, MinIO, Temporal, optional observability) via docker-compose, plus the Makefile targets to control it. Every later task that touches the database, object storage, messaging, or workflow orchestration assumes this stack is running locally — without it, nothing past task 03 can be developed or tested against real infra (per plans/docs/16-development-workflow.md §16.4's insistence on testing against real infra behavior, not mocks).

## Prerequisites
Task 01 (repo bootstrap — Makefile and directory skeleton must exist before this task adds to them).

## Scope / Deliverables
- `deploy/docker-compose.dev.yml` — the full local stack.
- `deploy/dev/` supporting config files as needed (e.g. MinIO bucket-init script, Temporal dynamic config override, Redpanda config overrides) — keep minimal.
- Makefile additions: `dev-up`, `dev-down`, `dev-logs` (convenience), `migrate` (wired to a no-op/placeholder until task 03 provides real migrations — do not leave the target absent), `seed` (placeholder until task 03/07 provide real fixture data — same rule: target must exist and be wired, content can be a stub that task 03/07 fill in without restructuring the target).
- `.env.example` at repo root (or `deploy/dev/.env.example`) documenting the local connection strings/ports/credentials the compose stack uses, consumed by `internal/platform/config` (task 01 skeleton, real loader wired in later tasks).

## Design Reference
- plans/docs/16-development-workflow.md §16.2 (the definitive table of services/substitutes/notes — follow it exactly, do not add or remove services) and §16.3 (config/secrets convention — env vars, fake/dev Vault or plain env-var secrets behind a feature flag for local dev).
- plans/docs/11-scalability-roadmap.md §12.2 Phase 0 (confirms Temporal is *not* required to be wired into case logic yet — see Non-Goals; the compose service can exist and boot without task 13's state machine depending on it).
- plans/docs/01-multi-tenancy.md §2.1-2.2 (RLS policies apply even in single-node local Postgres — this task doesn't write the policies, task 03 does, but the Postgres service here must be a plain vanilla instance, not Citus, so those RLS bugs surface locally).

## Implementation Notes
- Services in `docker-compose.dev.yml`, matching §16.2 table exactly:
  - `postgres`: official `postgres:16` (or current stable) image, single node, exposed on `5432`, a named volume for persistence across restarts, healthcheck (`pg_isready`), default db/user/password sourced from `.env.example` values (e.g. `jengine`/`jengine`/`jengine_dev` — clearly local-only, never used elsewhere).
  - `redpanda`: single-node Redpanda (`redpandadata/redpanda` image), Kafka-API-compatible, no ZooKeeper, exposed Kafka API port (e.g. `9092`) and admin/schema-registry port if the image bundles one — keep config minimal, single broker.
  - `redis`: single Redis instance (`redis:7`+), exposed `6379`.
  - `minio`: S3-compatible object storage, exposed API port + console port, an init step (either a compose `command`/entrypoint override or a one-shot `mc` sidecar container) that creates the buckets the app expects (e.g. `jengine-statements`, `jengine-audit-archive`) on startup — document that Object Lock/WORM behavior is mocked/skipped locally per §16.2, do not attempt to configure real Object Lock in dev MinIO.
  - `temporal`: `temporalio/auto-setup` image + the Temporal Web UI image, per §16.2 — bring up but do not wire any application code to it yet (task 13 works in Postgres-only state-machine mode per Phase 0 scope; Temporal is present in dev so its eventual introduction isn't blocked, not because MVP code calls it).
  - `observability` profile (optional, `docker compose --profile observability up`): Jaeger, Prometheus, Grafana — must NOT start with a bare `docker compose up`, only when the profile flag is passed. Use compose's `profiles:` key on these three services.
- All services on a single docker-compose network; use `depends_on` + healthchecks so `make dev-up` blocks until Postgres/Redis/MinIO report healthy before returning (Redpanda/Temporal can be best-effort healthchecked given slower boot).
- `make dev-up`: `docker compose -f deploy/docker-compose.dev.yml up -d` (plus bucket-init wait if needed).
- `make dev-down`: `docker compose -f deploy/docker-compose.dev.yml down` (add a `dev-down-volumes` or documented flag for `-v` separately — do not make the default `dev-down` destroy data, since iterative local dev depends on Postgres data surviving a restart).
- `make migrate`: for now (before task 03 exists), wire it to invoke whatever migration tool task 03 will choose (see task 03 — do not decide the migration tool in this task; leave the Makefile target shelling out to a `scripts/migrate.sh` file that task 03 populates, so this task doesn't need to be revisited). If task 03 hasn't landed yet in build order, `scripts/migrate.sh` can echo "no migrations yet" and exit 0.
- `make seed`: same deferred-content pattern — wire to `scripts/seed.sh`, populated for real once task 03 (schema) and task 07 (MT940 sample connector) exist; stub for now.
- `.env.example` should include: `POSTGRES_HOST/PORT/USER/PASSWORD/DB`, `REDIS_ADDR`, `REDPANDA_BROKERS`, `MINIO_ENDPOINT/ACCESS_KEY/SECRET_KEY/BUCKET_STATEMENTS/BUCKET_AUDIT`, `TEMPORAL_HOST_PORT`. Keep names consistent with whatever `internal/platform/config` will later expect (align with task 04/05's config needs where already known, e.g. Postgres DSN shape).

## Non-Goals / Guardrails
- Do not implement Citus/multi-node Postgres locally — §16.2 explicitly says plain single-node Postgres is the local substitute; Citus-specific behavior is not testable locally at MVP and that's an accepted tradeoff, not a gap to fix here.
- Do not implement a Kafka Connect cluster or Debezium CDC pipeline locally — explicitly out of scope per §16.2 ("no Kafka Connect cluster"). CDC/Debezium work belongs to V1 tasks (18+).
- Do not wire any `cmd/*` binary to actually connect to this stack yet — that begins in task 03+ as each module needs real infra. This task only makes the infra available and controllable via `make` targets.
- Do not attempt real S3 Object Lock/WORM enforcement in local MinIO — note it as mocked/skipped, per §16.2, and do not spend implementation effort making local WORM "real."
- Do not start the observability profile services by default — must be an explicit opt-in profile flag.
- Do not write real database schema/migrations here — that's task 03; this task's `migrate` target is a placeholder hook only.

## Definition of Done
- `make dev-up` brings up postgres, redpanda, redis, minio, temporal (+ UI) with no manual intervention; `docker compose ps` shows all healthy/running.
- `make dev-up` followed immediately by `docker compose --profile observability up -d` (as a second manual step, not via `dev-up` itself) brings up jaeger/prometheus/grafana additionally — confirming the profile gating works.
- `make dev-down` stops the stack without deleting the named Postgres volume (verified by bringing the stack back up and confirming previously inserted test data, e.g. via a manual `psql` insert before down/up, survives).
- A basic integration test (can live under `internal/platform/config` or a new `test/devstack_test.go`, gated behind a build tag or `testing.Short()` skip so it doesn't run in unit-only CI stages) using `testcontainers-go` or a plain connectivity check confirms Postgres/Redis/MinIO/Redpanda are reachable on the documented ports when the stack is up — this satisfies §16.4's "real infra, not mocks" principle from day one of infra tasks.
- Manual verification: a new contributor following `.env.example` + `make dev-up` can connect to Postgres via `psql` and to MinIO console via browser without additional undocumented steps.

## Common Pitfalls
- Making `make dev-down` equivalent to `docker compose down -v`, silently wiping the Postgres volume on every stop — this breaks iterative local development and contradicts the point of persisting local data across restarts.
- Standing up Citus images or multi-node Postgres "to be closer to production" — directly contradicts §16.2's explicit simplification instruction.
- Starting Jaeger/Prometheus/Grafana unconditionally in the default `dev-up` — must be profile-gated, not always-on.
- Wiring Debezium/Kafka Connect locally because streaming architecture docs (06) mention CDC — CDC is V1 scope (task 18+), not part of this MVP infra task.
- Hardcoding production-looking secrets/credentials into `docker-compose.dev.yml` instead of sourcing from `.env`/`.env.example` — keep local creds obviously local-only and overridable.
- Forgetting healthchecks and letting `make dev-up` return immediately while Postgres is still initializing, causing flaky "connection refused" errors in whichever task runs migrations next (task 03) — block on healthy status.
