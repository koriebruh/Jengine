#!/usr/bin/env bash
# plans/task/core/22: applies deploy/clickhouse/ddl.sql (Kafka Engine
# tables, ReplacingMergeTree detail tables, the three materialized
# views) to the local dev ClickHouse. Idempotent (every statement is
# CREATE ... IF NOT EXISTS) - safe to re-run after `docker compose up -d
# clickhouse`.
set -euo pipefail

CONTAINER="${CLICKHOUSE_CONTAINER:-jengine-clickhouse}"

echo "waiting for ClickHouse in ${CONTAINER} ..."
for _ in $(seq 1 30); do
  if docker exec "${CONTAINER}" clickhouse-client --query "SELECT 1" >/dev/null 2>&1; then
    break
  fi
  sleep 2
done

docker exec -i "${CONTAINER}" clickhouse-client --multiquery < deploy/clickhouse/ddl.sql
echo "ClickHouse DDL applied."
