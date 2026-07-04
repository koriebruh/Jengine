#!/usr/bin/env bash
set -euo pipefail

# Wired into `make seed` (plans/task/core/02). Real content lands here per
# plans/task/core/07: loads scripts/seed-data/incoming/sample.sta through
# the SFTP+MT940 connector path (docker-compose's sftp service) into the
# local dev stack, producing visible Statement/Transaction rows in
# Postgres. Requires `make dev-up` to already be running.

export SECRET_SFTP_DEV_PASSWORD="${SFTP_PASSWORD:-jengine_dev}"

go run ./cmd/ingestion-gateway -demo=seed
