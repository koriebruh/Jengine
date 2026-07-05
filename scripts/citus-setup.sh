#!/usr/bin/env bash
# plans/task/core/24: registers the two Citus worker nodes with the
# coordinator (citus_add_node) - required after `docker compose
# --profile citus up -d` before any create_distributed_table call can
# actually shard across more than the coordinator itself.
set -euo pipefail

COORDINATOR_CONTAINER="${CITUS_COORDINATOR_CONTAINER:-jengine-citus-coordinator}"

echo "waiting for Citus coordinator..."
for _ in $(seq 1 30); do
  if docker exec "${COORDINATOR_CONTAINER}" pg_isready -U jengine -d jengine >/dev/null 2>&1; then
    break
  fi
  sleep 2
done

echo "registering worker nodes with the coordinator (idempotent - ignores 'already exists' on re-run)..."
# Explicit ::int cast on the port - a bare integer literal fails
# Postgres's overload resolution against citus_add_node's multi-
# default-argument signature ("function does not exist") without it,
# found via direct testing against a real cluster.
docker exec "${COORDINATOR_CONTAINER}" psql -U jengine -d jengine -c \
  "SELECT citus_add_node('jengine-citus-worker-1', 5432::int);" || true
docker exec "${COORDINATOR_CONTAINER}" psql -U jengine -d jengine -c \
  "SELECT citus_add_node('jengine-citus-worker-2', 5432::int);" || true

echo "cluster nodes:"
docker exec "${COORDINATOR_CONTAINER}" psql -U jengine -d jengine -c "SELECT * FROM citus_get_active_worker_nodes();"
