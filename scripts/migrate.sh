#!/usr/bin/env bash
set -euo pipefail

# Real implementation (plans/task/core/03). Migration tool: golang-migrate
# (github.com/golang-migrate/migrate/v4/cmd/migrate) - SQL-file-based,
# up/down pairs, no ORM, no extra runtime dependency in the app binary. See
# migrations/0001_init_schema.up.sql header for the same note.
#
# Usage:
#   scripts/migrate.sh            # runs `up` (all pending migrations)
#   scripts/migrate.sh up 1       # one step forward
#   scripts/migrate.sh down 1     # one step back
#   scripts/migrate.sh version    # current schema version
#   scripts/migrate.sh <anything> # forwarded to `migrate` verbatim
#
# Install the CLI if missing:
#   go install -tags 'postgres' github.com/golang-migrate/migrate/v4/cmd/migrate@latest

if [ -f .env ]; then
  set -a
  # shellcheck disable=SC1091
  source .env
  set +a
fi

MIGRATE_BIN="${MIGRATE_BIN:-migrate}"
if ! command -v "$MIGRATE_BIN" >/dev/null 2>&1; then
  gopath_bin="$(go env GOPATH 2>/dev/null)/bin/migrate"
  if [ -x "$gopath_bin" ]; then
    MIGRATE_BIN="$gopath_bin"
  else
    echo "migrate CLI not found on PATH or at \$(go env GOPATH)/bin/migrate." >&2
    echo "Install: go install -tags 'postgres' github.com/golang-migrate/migrate/v4/cmd/migrate@latest" >&2
    exit 1
  fi
fi

POSTGRES_HOST="${POSTGRES_HOST:-localhost}"
POSTGRES_PORT="${POSTGRES_PORT:-5432}"
POSTGRES_USER="${POSTGRES_USER:-jengine}"
POSTGRES_PASSWORD="${POSTGRES_PASSWORD:-jengine_dev}"
POSTGRES_DB="${POSTGRES_DB:-jengine}"

DSN="postgres://${POSTGRES_USER}:${POSTGRES_PASSWORD}@${POSTGRES_HOST}:${POSTGRES_PORT}/${POSTGRES_DB}?sslmode=disable"

if [ "$#" -eq 0 ]; then
  set -- up
fi

exec "$MIGRATE_BIN" -path migrations -database "$DSN" "$@"
