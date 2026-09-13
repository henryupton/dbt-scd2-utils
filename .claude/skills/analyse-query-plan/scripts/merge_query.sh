#!/usr/bin/env bash
# Print (to stdout) the inner SELECT of a custom-materialization MERGE -- the
# part inside `using ( ... ) AS DBT_INTERNAL_SOURCE` -- from a model's last-run
# compiled artifact, with the dropped `<model>__dbt_tmp` temp relation rewritten
# to the base table so the query EXPLAINs standalone.
#
# Run the model first so target/run/.../<model>.sql exists.
#
# Usage: merge_query.sh <model_name> > /tmp/q.sql
set -euo pipefail

MODEL="${1:?usage: merge_query.sh <model_name>}"
REPO="$(git -C "$(dirname "$0")" rev-parse --show-toplevel)"

ART="$(find "$REPO/integration_tests/target/run" -name "${MODEL}.sql" 2>/dev/null | head -1)"
[ -n "$ART" ] || { echo "no run artifact for '$MODEL' -- run the model first" >&2; exit 1; }

S=$(grep -n '^using (' "$ART" | head -1 | cut -d: -f1)
E=$(grep -n ') AS DBT_INTERNAL_SOURCE' "$ART" | head -1 | cut -d: -f1)
[ -n "$S" ] && [ -n "$E" ] || {
  echo "no MERGE 'using ( ... ) AS DBT_INTERNAL_SOURCE' block in $ART" >&2
  echo "(is this an incremental run of an incremental_scd2 model?)" >&2
  exit 1; }

# Inner query is between the two markers; strip the __dbt_tmp suffix so the
# reference resolves to the persisted base table.
awk -v s=$((S+1)) -v e=$((E-1)) 'NR>=s && NR<=e' "$ART" | sed 's/__dbt_tmp//g'
