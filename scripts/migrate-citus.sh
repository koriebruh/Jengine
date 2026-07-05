#!/usr/bin/env bash
# plans/task/core/24: applies migrations/citus/distribution.sql (Citus
# distribution - create_distributed_table/create_reference_table calls
# that only work against a Citus-enabled cluster) against the opt-in
# Citus coordinator, SEPARATELY from scripts/migrate.sh's main
# migrations/ sequence. Run `make citus-up` first (which runs the base
# schema migrations against the coordinator before this).
#
# Applied via `psql -f`, NOT golang-migrate: golang-migrate's postgres
# driver sends a migration file as a single PREPARE, and Postgres
# rejects "multiple commands" in one PREPARE - found via direct
# testing, this file's mix of plain DDL and Citus's own SELECT
# function calls hits exactly that limitation. `psql -f` uses the
# simple query protocol instead, which allows a multi-statement file,
# matching how this file's own statements were developed/verified
# interactively. distribution.sql tracks its own application via a
# schema_migrations_citus marker table (see the file's own header) and
# every ALTER uses IF EXISTS/re-runnable forms, so re-running this
# script is safe.
set -euo pipefail

CITUS_COORDINATOR_CONTAINER="${CITUS_COORDINATOR_CONTAINER:-jengine-citus-coordinator}"

docker exec -i "${CITUS_COORDINATOR_CONTAINER}" psql -U jengine -d jengine -v ON_ERROR_STOP=1 -f - < migrations/citus/distribution.sql

# pg_dist_authinfo: Citus opens its OWN connections to worker nodes on
# behalf of whichever role issued the original query - password-auth'd
# roles need an explicit entry here or cross-node queries fail with
# "no password supplied"/"password authentication failed" even though a
# direct psql connection as that role works fine (found via direct
# testing - this is Citus-specific connection-forwarding behavior, not
# a config issue with the roles themselves). nodeid=0 applies to all
# nodes. Requires the role to already exist (pg_dist_authinfo has a
# role_exists CHECK constraint) - both jengine (superuser) and
# jengine_app exist by this point (base migrations already ran).
echo "configuring cross-node auth for jengine/jengine_app..."
docker exec "${CITUS_COORDINATOR_CONTAINER}" psql -U jengine -d jengine -c \
  "INSERT INTO pg_dist_authinfo (nodeid, rolename, authinfo) VALUES (0, 'jengine', 'password=jengine_dev') ON CONFLICT DO NOTHING;"
docker exec "${CITUS_COORDINATOR_CONTAINER}" psql -U jengine -d jengine -c \
  "INSERT INTO pg_dist_authinfo (nodeid, rolename, authinfo) VALUES (0, 'jengine_app', 'password=jengine_app_dev') ON CONFLICT DO NOTHING;"

# jengine_app must also exist as a LOGIN role on each worker (Citus
# rejects direct CREATE ROLE on a worker with "operation is not allowed
# on this node" unless citus.enable_ddl_propagation is disabled locally
# first - found via direct testing).
echo "ensuring jengine_app role exists on worker nodes..."
for worker in jengine-citus-worker-1 jengine-citus-worker-2; do
  docker exec "${worker}" psql -U jengine -d jengine -c \
    "SET citus.enable_ddl_propagation = off; DO \$\$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'jengine_app') THEN CREATE ROLE jengine_app LOGIN PASSWORD 'jengine_app_dev'; END IF; END \$\$;" || true
done
