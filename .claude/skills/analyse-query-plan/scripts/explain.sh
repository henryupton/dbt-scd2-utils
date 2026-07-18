#!/usr/bin/env bash
# Run EXPLAIN (USING TEXT) on a SQL file against the dev Snowflake target and
# print the operator tree plus a partition-pruning summary.
#
# dbt show cannot EXPLAIN (it wraps the query in a subquery), so this writes a
# throwaway run-operation macro that calls run_query('explain using text ...')
# and logs each plan row, then removes it.
#
# Usage: explain.sh <path-to-sql-file>
#   env overrides: DBT (binary), DBT_PROFILE (default), DBT_TARGET (dev)
set -euo pipefail

SQL_FILE="${1:?usage: explain.sh <sql-file>}"
DBT="${DBT:-$HOME/.local/bin/dbt}"
PROFILE="${DBT_PROFILE:-default}"
TARGET="${DBT_TARGET:-dev}"

REPO="$(git -C "$(dirname "$0")" rev-parse --show-toplevel)"
PROJ="$REPO/integration_tests"
MACRO_DIR="$PROJ/macros"
MACRO="$MACRO_DIR/_explain_scratch.sql"

mkdir -p "$MACRO_DIR"
cleanup() { rm -f "$MACRO"; rmdir "$MACRO_DIR" 2>/dev/null || true; }
trap cleanup EXIT

{
  echo '{% macro _explain_scratch() %}'
  echo '{% set q %}{% raw %}'
  cat "$SQL_FILE"
  echo '{% endraw %}{% endset %}'
  echo '{% if execute %}'
  echo "{% set res = run_query('explain using text ' ~ q) %}"
  echo '{% for row in res.rows %}{{ log(row[-1], info=True) }}{% endfor %}'
  echo '{% endif %}'
  echo '{% endmacro %}'
} > "$MACRO"

OUT="$(cd "$PROJ" && "$DBT" run-operation _explain_scratch --profile "$PROFILE" --target "$TARGET" 2>&1)"

echo "== operator tree (indent = child; truncated to 200 cols) =="
printf '%s\n' "$OUT" | grep -E '\->' | cut -c1-200 || {
  echo "(no plan rows -- raw output below)"; printf '%s\n' "$OUT" | tail -30; exit 1; }

echo
echo "== pruning (TableScan partitions; assigned < total means pruned) =="
printf '%s\n' "$OUT" | grep -oE '\->TableScan[^{]*\{[^}]*\}' || echo "(no TableScan partition stats found)"
