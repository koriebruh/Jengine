.PHONY: build test lint vet fmt tidy clean lint-tenancy \
	dev-up dev-down dev-down-volumes dev-logs migrate seed create-topics \
	streaming-up register-connectors \
	test-unit test-integration test-golden ci \
	web-dev web-build web-lint

# --- Task 01: core build/lint/test automation ---------------------------

build: ## Build every cmd/* binary; fails fast on any compile error.
	go build ./cmd/...

vet:
	go vet ./...

lint: vet lint-tenancy
	golangci-lint run ./...

# Task 04: real go/analysis-based checker, superseding task 01's grep
# script (plans/task/core/04 Implementation Notes) now that a real
# repository-layer convention exists. Scope is the invocation's package
# list, not hardcoded in the analyzer - expand this as task 05+ add more
# repository packages.
lint-tenancy:
	go run ./internal/platform/lint/tenantcheck/cmd/tenantcheck ./internal/storage/postgres/...

test:
	go test -race ./...

tidy:
	go mod tidy

# gofmt is the formatting tool of record (not goimports) - keep it that way
# unless a later task deliberately adopts goimports and updates this target.
fmt:
	gofmt -l -w .

clean:
	rm -rf bin/

# --- Task 02: local dev infrastructure -----------------------------------

COMPOSE := docker compose -f docker-compose.yaml

dev-up: ## Bring up postgres/redpanda/redis/minio/temporal(+ui); blocks until healthy.
	$(COMPOSE) up -d
	$(COMPOSE) ps

dev-down: ## Stop the stack WITHOUT deleting the postgres volume.
	$(COMPOSE) down

dev-down-volumes: ## Stop the stack AND delete all volumes - destroys local data.
	$(COMPOSE) down -v

dev-logs:
	$(COMPOSE) logs -f

migrate: ## Runs migrations/*.sql via golang-migrate (plans/task/core/03).
	./scripts/migrate.sh

seed: ## Loads the sample MT940 file through the SFTP+MT940 connector path (plans/task/core/07).
	./scripts/seed.sh

create-topics: ## Creates the Redpanda topic layout (plans/task/core/18, deploy/redpanda/topics.yaml).
	./scripts/create-topics.sh

streaming-up: ## Brings up Kafka Connect + Debezium on top of the base stack (plans/task/core/18).
	$(COMPOSE) --profile streaming up -d
	$(COMPOSE) ps

register-connectors: ## Registers the Debezium outbox-event-router connector (plans/task/core/18).
	./scripts/register-debezium-connectors.sh

# --- Task 17: testing harness + CI-equivalent local run ------------------

test-unit:
	go test -race -short ./...

# Real convention across this codebase is testing.Short()-gated skip
# (every integration test starts "if testing.Short() { t.Skip(...) }"),
# not a Integration-name/-tags=integration convention - this target used
# to specify a filter matching zero tests anywhere in the repo (found
# during plans/task/core/17's own review: it had been silently running
# nothing all along). Omitting -short is what actually includes the
# Docker-backed integration tests test-unit's -short explicitly excludes.
test-integration:
	go test -race ./...

test-golden:
	go test -race ./internal/matching/core/... -run Golden

# Local equivalent of the CI pipeline stages in plans/docs/16-development-workflow.md §16.5.
ci: vet lint test build
	scripts/check-migration-safety.sh

# --- Task 01 (frontend): web/ Next.js app - single `make` entrypoint ------

web-dev: ## Boots the Next.js dev server (plans/task/frontend/01).
	cd web && npm run dev

web-build: ## Production build, zero TypeScript errors required.
	cd web && npm run build

web-lint: ## ESLint check.
	cd web && npm run lint
