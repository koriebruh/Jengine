#!/usr/bin/env bash
set -euo pipefail

# Proves check_tenant_id.sh actually catches the violation fixture and
# passes the compliant one. This IS the "test" required by
# plans/task/core/01's Definition of Done - not a checklist item.

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
checker="$script_dir/check_tenant_id.sh"

tmp_ok=$(mktemp -d)
tmp_bad=$(mktemp -d)
trap 'rm -rf "$tmp_ok" "$tmp_bad"' EXIT

cp "$script_dir/testdata/ok.go" "$tmp_ok/ok.go"
cp "$script_dir/testdata/violation.go" "$tmp_bad/violation.go"

result=0

if "$checker" "$tmp_ok" >/tmp/tenant_id_check_ok.out 2>&1; then
  echo "PASS: compliant fixture (ok.go) passes the checker"
else
  echo "FAIL: compliant fixture (ok.go) should have passed but didn't:"
  cat /tmp/tenant_id_check_ok.out
  result=1
fi

if "$checker" "$tmp_bad" >/tmp/tenant_id_check_bad.out 2>&1; then
  echo "FAIL: violation fixture (violation.go) should have been caught but the checker passed it"
  result=1
else
  echo "PASS: violation fixture (violation.go) correctly caught"
fi

exit "$result"
