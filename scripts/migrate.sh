#!/usr/bin/env bash
set -euo pipefail

# Wired into `make migrate` (plans/task/core/02). This is a deferred-content
# placeholder: plans/task/core/03 picks the migration tool and populates
# migrations/*.sql, then rewrites this script to actually run it. The
# `make migrate` target itself does not need to change when that happens.

if [ -z "$(ls -A migrations 2>/dev/null | grep -v '.gitkeep')" ]; then
  echo "no migrations yet (see plans/task/core/03)"
  exit 0
fi

echo "migrations/*.sql present but scripts/migrate.sh has not been updated by plans/task/core/03 to run them yet" >&2
exit 1
