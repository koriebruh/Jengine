#!/usr/bin/env bash
set -euo pipefail

# Checks that every method on a *Repository receiver has an explicit
# tenantID (or TenantContext) parameter in its signature. Grep/awk-based
# per plans/docs/01-multi-tenancy.md §2.2 and plans/task/core/17's explicit
# "custom or grep-based" allowance - deliberately not a full go/analysis
# linter (see plans/task/core/17 Non-Goals).
#
# Usage:
#   scripts/lint/check_tenant_id.sh                 # scans internal/storage/postgres
#   scripts/lint/check_tenant_id.sh <dir> [<dir>...] # scans given dirs instead
#
# No repository code exists yet as of task 01/17 - this is a no-op pass
# when internal/storage/postgres has no matching methods. Task 05 must
# make its repository code satisfy this check, not rewrite the check.

scan_dirs=("$@")
if [ ${#scan_dirs[@]} -eq 0 ]; then
  scan_dirs=("internal/storage/postgres")
fi

violations=0

for dir in "${scan_dirs[@]}"; do
  [ -d "$dir" ] || continue
  while IFS= read -r -d '' file; do
    if ! awk '
      /^func \(.*\*?[A-Za-z0-9_]*Repository\) [A-Za-z0-9_]+\(/ {
        buf = $0
        in_sig = 1
        if ($0 ~ /\{[[:space:]]*$/) {
          in_sig = 0
          check(buf)
        }
        next
      }
      in_sig {
        buf = buf "\n" $0
        if ($0 ~ /\{[[:space:]]*$/) {
          in_sig = 0
          check(buf)
        }
      }
      function check(sig) {
        if (sig !~ /tenantID/ && sig !~ /TenantContext/) {
          print FILENAME ": repository method missing tenantID/TenantContext parameter:"
          print sig
          bad = 1
        }
      }
      END { if (bad) exit 1 }
    ' "$file"; then
      violations=1
    fi
  done < <(find "$dir" -name '*.go' -print0)
done

if [ "$violations" -ne 0 ]; then
  echo "tenant-id lint check: FAILED - see violations above" >&2
  exit 1
fi

echo "tenant-id lint check: OK"
