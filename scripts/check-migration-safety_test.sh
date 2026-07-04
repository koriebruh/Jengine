#!/usr/bin/env bash
set -euo pipefail

# Proves check-migration-safety.sh catches a deliberately-breaking migration
# fixture and passes both a compliant expand-only one and a breaking-but-
# acknowledged one. This IS the "test" required by plans/task/core/03's
# Definition of Done - not a checklist item.

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
checker="$script_dir/check-migration-safety.sh"

result=0

echo "--- ok fixtures (expand-only + acknowledged drop) ---"
if MIGRATIONS_DIR="$script_dir/testdata/migration_safety_ok" "$checker" >/tmp/migsafe_ok.out 2>&1; then
  echo "PASS: compliant fixtures pass the checker"
else
  echo "FAIL: compliant fixtures should have passed but didn't:"
  cat /tmp/migsafe_ok.out
  result=1
fi

echo "--- violation fixture (bare DROP COLUMN, no marker) ---"
if MIGRATIONS_DIR="$script_dir/testdata/migration_safety_violation" "$checker" >/tmp/migsafe_bad.out 2>&1; then
  echo "FAIL: violation fixture should have been caught but the checker passed it"
  result=1
else
  echo "PASS: violation fixture correctly caught"
fi

exit "$result"
