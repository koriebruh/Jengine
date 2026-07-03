# Task 01: Repo Bootstrap and Tooling

## Goal
Establish the Go module, repository directory skeleton, and developer tooling (build/lint/test automation) that every subsequent task builds on top of. This task produces no business logic — it produces the scaffolding that makes "one command to build, one command to lint, one command to test" true from day one, so later tasks can focus purely on their own scope instead of re-deriving project layout or fighting inconsistent tooling. Getting this structurally right up front is what keeps the "modular monolith → future microservice extraction" path (plans/docs/00-overview-and-architecture.md §1.1) viable later.

## Prerequisites
None. This is the first task in the build sequence.

## Scope / Deliverables
- `go.mod` / `go.sum` at repo root, module path `github.com/jengine/jengine` (or equivalent — pick one and use it consistently; do not leave it as a placeholder).
- `cmd/` directory with one subdirectory per deployable binary, each containing a `main.go` stub that only prints a "not yet implemented" boot message and exits 0 (real wiring happens in later tasks):
  - `cmd/coreapi/main.go`
  - `cmd/ingestion-gateway/main.go`
  - `cmd/matching-batch/main.go`
  - `cmd/matching-stream/main.go`
  - `cmd/webhook-dispatcher/main.go`
  - `cmd/api-gateway/main.go`
- `internal/` directory skeleton (empty packages with a single `doc.go` per package stating its purpose in one line, per plans/docs/16-development-workflow.md §16.1):
  - `internal/tenancy/`
  - `internal/rules/`
  - `internal/cases/`
  - `internal/audit/`
  - `internal/notify/`
  - `internal/ingestion/connector/`, `internal/ingestion/parsers/`, `internal/ingestion/mapping/`
  - `internal/matching/core/`, `internal/matching/rules/`, `internal/matching/similarity/`
  - `internal/storage/postgres/`, `internal/storage/clickhouse/`
  - `internal/platform/config/`, `internal/platform/observability/`, `internal/platform/authz/`
- `proto/jengine/v1/` directory (empty except a placeholder `.gitkeep` or a trivial `README.md` noting this is populated starting task 10+; do not author `.proto` files here — that begins with the matching engine tasks).
- `migrations/` directory (empty, populated by task 03).
- `deploy/helm/`, `deploy/docker-compose.dev.yml` placeholder path (real content in task 02).
- `web/` directory placeholder (real content is frontend tasks, out of scope here — create only an empty directory with `.gitkeep` so the path exists).
- `Makefile` at repo root with targets: `build`, `test`, `lint`, `vet`, `fmt`, `tidy`, `clean`. (`dev-up`/`dev-down`/`migrate`/`seed` targets are added in task 02 — do not add stub versions here that task 02 has to rework.)
- `.golangci.yml` — lint configuration.
- `.gitignore` covering Go build artifacts, `.env`, IDE files, `/bin`.
- A custom lint check (script or golangci-lint rule) enforcing the "every repository query requires an explicit tenant_id argument" convention — see Implementation Notes below. It is acceptable for this task to add the check as a no-op/pass-through if no repository code exists yet, as long as the mechanism (script + Makefile/CI wiring) exists and is exercised by a trivial fixture test proving it *would* catch a violation.

## Design Reference
- plans/docs/16-development-workflow.md §16.1 (repository layout, module-boundary rule), §16.3 (12-factor config convention, manual constructor injection — no DI framework), §16.5 (CI pipeline stage ordering: vet/lint → test -race → buf breaking → migration lint → build all cmd/*).
- plans/docs/00-overview-and-architecture.md §1.1 (modular monolith rationale — why `internal/` visibility boundaries matter).
- plans/docs/01-multi-tenancy.md §2.2 (the tenant_id-in-every-query lint rule this task must scaffold the enforcement mechanism for; task 04 is where the actual `TenantContext` code that satisfies it gets built).

## Implementation Notes
- Go version: target Go 1.23+ per plans/docs/00-overview-and-architecture.md §1.3. Set `go 1.23` (or newer stable at implementation time) in `go.mod`.
- Each `cmd/*/main.go` stub should look like:
  ```go
  package main

  import (
      "fmt"
      "os"
  )

  func main() {
      fmt.Println("coreapi: not yet implemented (see task/core/*)")
      os.Exit(0)
  }
  ```
  Replace `coreapi` with the correct binary name per file. This keeps `make build` green from commit one.
- `doc.go` per internal package, e.g. `internal/tenancy/doc.go`:
  ```go
  // Package tenancy implements TenantContext propagation and tenant/shard
  // routing. See plans/docs/01-multi-tenancy.md.
  package tenancy
  ```
- Makefile targets should shell out to standard tools, not reinvent them:
  - `lint`: `golangci-lint run ./...`
  - `vet`: `go vet ./...`
  - `test`: `go test -race ./...`
  - `build`: loop or `go build ./cmd/...` (fails fast on any compile error across the whole module set, matching CI stage 5 in §16.5).
  - `tidy`: `go mod tidy`
  - `fmt`: `gofmt -l -w .` (or `goimports` if adopted — pick one, document the choice in a Makefile comment, don't leave both half-wired).
- `.golangci.yml`: enable at minimum `govet`, `staticcheck`, `errcheck`, `unused`, `gosimple`, `ineffassign`. Do not enable overly aggressive style-only linters (e.g. strict line-length) that would generate noise unrelated to correctness — keep the config lean; later tasks may tighten it, don't gold-plate here.
- Tenant-id-in-query lint check: implement as a standalone script (e.g. `scripts/lint/check_tenant_id.sh` or a small Go static-analysis tool under `internal/platform/lint/` if you prefer an `x/tools/go/analysis`-based checker) that scans for repository-layer function calls/definitions matching a naming convention (to be finalized once task 05's repository interfaces exist) and flags any that lack a `tenantID` (or `TenantContext`) parameter. Since no repository code exists yet at this task, the concrete grep/AST pattern only needs to be provable against a fixture file checked into the script's own test directory (e.g. `scripts/lint/testdata/violation.go` containing a deliberately-bad function, and `scripts/lint/testdata/ok.go` containing a compliant one) — wire it into the `lint` Makefile target and a CI step placeholder. Task 05 must not have to rewrite this check, only feed it real code.
- Wire the CI pipeline stage order from §16.5 into whatever CI config exists (GitHub Actions or equivalent — pick GitHub Actions under `.github/workflows/ci.yml` as the default choice since no CI system is otherwise specified): vet+lint → test -race → (buf breaking — skip/no-op until proto files exist, task 10+) → migration lint (no-op until task 03) → build all cmd/*.
- Manual dependency injection convention (§16.3): do not introduce Wire/Fx/dig at this stage. Note this explicitly as a comment in the root `README.md` or a `docs/CONTRIBUTING.md` note if one is created — but do not create new documentation files beyond what's explicitly needed for tooling (e.g. a `.golangci.yml` needs no prose doc).

## Non-Goals / Guardrails
- Do not write any actual business logic, domain structs, or database code — that starts at task 03/05.
- Do not stand up docker-compose, Postgres, or any other infrastructure service — that is task 02.
- Do not author `.proto` files — proto contracts are introduced starting with the matching engine (task 10+) and API layer (task 15) tasks.
- Do not add a dependency-injection framework (Wire/Fx/dig) — explicitly rejected at this scale per §16.3.
- Do not pick CockroachDB, microservices-from-day-one, or any other option the design docs explicitly rejected — this is a spec-following task, not an architecture-redesign task.
- Do not build the real tenant-id lint checker against actual repository code (it doesn't exist yet) — a fixture-provable mechanism is sufficient; task 04/05 authors must ensure their code satisfies it, not this task.

## Definition of Done
- `make build` succeeds and produces all `cmd/*` binaries.
- `make vet` and `make lint` both pass cleanly on the scaffolded tree.
- `make test` runs (even with zero real tests present, `go test ./...` must exit 0 — trivial doc.go-only packages compile fine).
- The tenant-id lint check script has its own test that proves it fails on the deliberately-bad fixture and passes on the compliant fixture (this is the "test," not a checklist item).
- CI config file is present and runs the stages in the order specified in §16.5 (proto/migration stages may be no-ops at this point, but must not be silently absent — they should exist as explicit skip/pass steps so later tasks only need to fill them in, not add them).
- Manual verification: a fresh clone + `make build && make test && make lint` succeeds with no manual setup steps beyond having Go installed.

## Common Pitfalls
- Naming the module something inconsistent across files (e.g. mixing `jengine/jengine` and `github.com/jengine/jengine` import paths) — pick one module path in `go.mod` and use it verbatim everywhere.
- Creating `internal/matching/`, `internal/ingestion/`, etc. as flat packages instead of the nested subpackage structure specified in §16.1 (`internal/matching/core`, `internal/matching/rules`, `internal/matching/similarity` are three separate packages, not one `matching` package with three files) — this nesting is what the module-boundary rule depends on.
- Adding real docker-compose or database config here "since it's convenient" — that's task 02's scope; keep this task infra-free.
- Introducing Wire/Fx eagerly because it "seems like best practice" — the design doc explicitly rejects this at current scale (§16.3).
- Skipping the `cmd/api-gateway` binary because §16.1 notes it "can stay colocated with coreapi at MVP" — still scaffold the directory/stub; the note is about deployment topology later, not about omitting the entrypoint now.
- Over-configuring `.golangci.yml` with strict stylistic rules that will generate hundreds of warnings once real code lands in tasks 03+ — keep it correctness-focused per Implementation Notes.
