#!/usr/bin/env bash
# plans/task/core/18: registers the outbox-event-router connector with
# Kafka Connect's REST API. plans/task/core/22 extends this SAME script
# (and the SAME Kafka Connect cluster - no second one stood up) with the
# table-level CDC connectors feeding ClickHouse. Run after `docker
# compose --profile streaming up -d` (kafka-connect must be healthy
# first).
set -euo pipefail

CONNECT_URL="${KAFKA_CONNECT_URL:-http://localhost:8083}"

echo "waiting for Kafka Connect at ${CONNECT_URL} ..."
for _ in $(seq 1 30); do
  if curl -sf "${CONNECT_URL}/connectors" >/dev/null 2>&1; then
    break
  fi
  sleep 2
done

register() {
  local name="$1" config_file="$2"
  echo "registering ${name} (idempotent: delete-then-create, no jq dependency) ..."
  curl -sf -X DELETE "${CONNECT_URL}/connectors/${name}" >/dev/null 2>&1 || true
  curl -sf -X POST -H "Content-Type: application/json" \
    --data "@${config_file}" \
    "${CONNECT_URL}/connectors"
  echo
}

register jengine-outbox-connector deploy/debezium/outbox-connector.json
register jengine-cdc-transactions-connector deploy/debezium/cdc-transactions-connector.json
register jengine-cdc-match-results-connector deploy/debezium/cdc-match-results-connector.json
register jengine-cdc-cases-connector deploy/debezium/cdc-cases-connector.json

# Registration needs a moment to actually start each connector's task -
# a status check immediately after POST can transiently 404/fail even
# though registration succeeded.
sleep 3
for name in jengine-outbox-connector jengine-cdc-transactions-connector jengine-cdc-match-results-connector jengine-cdc-cases-connector; do
  echo "status: ${name}"
  curl -sf "${CONNECT_URL}/connectors/${name}/status"
  echo
done
