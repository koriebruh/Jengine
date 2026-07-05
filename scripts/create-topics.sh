#!/usr/bin/env bash
# plans/task/core/18: creates every topic in deploy/redpanda/topics.yaml
# via rpk against the local dev Redpanda (task 02). Explicit rpk commands
# rather than a YAML-parsing step (no yq dependency in this repo) -
# topics.yaml is the readable source of truth for the layout/rationale,
# this script is its literal implementation; keep the two in sync by
# hand, they change together.
set -euo pipefail

RPK="docker exec jengine-redpanda rpk"

create_topic() {
  local name="$1" partitions="$2" retention_ms="$3" cleanup="${4:-delete}"
  echo "creating topic: ${name} (partitions=${partitions} retention_ms=${retention_ms} cleanup=${cleanup})"
  $RPK topic create "${name}" \
    --partitions "${partitions}" \
    --config "retention.ms=${retention_ms}" \
    --config "cleanup.policy=${cleanup}" \
    2>&1 | grep -v "already exists" || true
}

create_topic ingestion.raw.default 50 604800000
create_topic normalized.transactions.default 50 2592000000
create_topic matching.results.default 50 7776000000
create_topic case.events.default 50 31536000000
create_topic audit.events 50 -1 compact
create_topic webhook.outbox 50 604800000
create_topic dlq.format_parse 50 7776000000
create_topic dlq.field_mapping 50 7776000000
create_topic dlq.validation 50 7776000000

echo "topics:"
$RPK topic list
