#!/usr/bin/env bash
set -euo pipefail

# Migration lint per plans/docs/16-development-workflow.md §16.5 stage 4
# and plans/task/core/17. Flags DROP COLUMN / DROP TABLE / SET NOT NULL in
# changed migrations/*.sql files that aren't already acknowledged with a
# human override comment. Pattern-matching, not exhaustively smart - per
# task 17's explicit bar: "false positives requiring a human override
# comment are acceptable, false negatives on truly dangerous migrations
# are not."
#
# Override: add a comment containing `migration-safety: acknowledged`
# within 3 lines above the flagged statement if the change really is safe
# (e.g. a prior deprecation migration already ran for that column/table).
#
# No migrations exist yet as of plans/task/core/01/02/17 - this is a
# no-op pass until plans/task/core/03 adds real migrations/*.sql files.

BASE_REF="${1:-origin/main}"

find_changed_migrations() {
  if git rev-parse --verify "$BASE_REF" >/dev/null 2>&1 \
    && [ "$(git rev-parse "$BASE_REF")" != "$(git rev-parse HEAD)" ]; then
    git diff --name-only --diff-filter=ACMR "$BASE_REF"...HEAD -- 'migrations/*.sql' 2>/dev/null
  fi
}

changed_files="$(find_changed_migrations || true)"
if [ -z "$changed_files" ]; then
  # No base ref to diff against (or nothing changed there) - fall back to
  # linting whatever migrations currently exist, so local/first-ever runs
  # still do something useful instead of trivially passing.
  changed_files="$(find migrations -name '*.sql' 2>/dev/null || true)"
fi

if [ -z "$changed_files" ]; then
  echo "migration-safety check: no migrations to check (see plans/task/core/03)"
  exit 0
fi

violations=0

while IFS= read -r file; do
  [ -f "$file" ] || continue
  while IFS=: read -r line_no line_content; do
    [ -z "$line_no" ] && continue
    start=$(( line_no > 3 ? line_no - 3 : 1 ))
    context=$(sed -n "${start},${line_no}p" "$file")
    if echo "$context" | grep -qi 'migration-safety: acknowledged'; then
      continue
    fi
    echo "$file:$line_no: $line_content"
    violations=1
  done < <(grep -Ein 'drop[[:space:]]+column|drop[[:space:]]+table|alter[[:space:]]+column.*set[[:space:]]+not[[:space:]]+null' "$file" || true)
done <<< "$changed_files"

if [ "$violations" -ne 0 ]; then
  echo "migration-safety check: FAILED - possible breaking change without expand-contract evidence (plans/docs/10-observability-reliability.md §11.5). Add a 'migration-safety: acknowledged' comment within 3 lines above the statement if this is genuinely safe." >&2
  exit 1
fi

echo "migration-safety check: OK"
