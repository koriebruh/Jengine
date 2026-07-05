#!/usr/bin/env bash
# plans/task/core/18: registers the outbox-event-router connector with
# Kafka Connect's REST API. Run after `docker compose --profile
# streaming up -d` (kafka-connect must be healthy first).
set -euo pipefail

CONNECT_URL="${KAFKA_CONNECT_URL:-http://localhost:8083}"

echo "waiting for Kafka Connect at ${CONNECT_URL} ..."
for _ in $(seq 1 30); do
  if curl -sf "${CONNECT_URL}/connectors" >/dev/null 2>&1; then
    break
  fi
  sleep 2
done

echo "registering jengine-outbox-connector (idempotent: delete-then-create, no jq dependency) ..."
curl -sf -X DELETE "${CONNECT_URL}/connectors/jengine-outbox-connector" >/dev/null 2>&1 || true
curl -sf -X POST -H "Content-Type: application/json" \
  --data @deploy/debezium/outbox-connector.json \
  "${CONNECT_URL}/connectors"
echo

# The task needs a moment to actually start after registration - a
# status check immediately after POST can transiently 404/fail even
# though registration succeeded.
sleep 3
echo "connector status:"
curl -sf "${CONNECT_URL}/connectors/jengine-outbox-connector/status"
echo
