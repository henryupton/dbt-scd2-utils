#!/usr/bin/env bash
#
# Negative tests for the scd materialization.
#
# Each fixture model is a misconfiguration that must raise a compiler error, and
# is disabled by default via enabled=var('run_negative_tests', false) so it never
# affects normal runs. This script enables them and asserts each one fails with
# its expected error message.
#
#   - customers_scd1_deleted_at_invalid : deleted_at_column on a type 0/1 model
#   - customers_scd_invalid_type        : an unsupported scd_type (3)
#
# Usage:
#   ./test_scd_negative.sh [profile]
#   ./test_scd_negative.sh integration_tests

set -uo pipefail
cd "$(dirname "$0")"

PROFILE="${1:-}"

# Each case: "<model>:::<expected error substring>"
CASES=(
    "customers_scd1_deleted_at_invalid:::deletion tracking is not supported for SCD types 0 and 1"
    "customers_scd_invalid_type:::scd_type must be 0, 1 or 2"
)

rc=0
for case in "${CASES[@]}"; do
    model="${case%%:::*}"
    expected="${case##*:::}"
    echo "[negative-test] Running ${model} (expecting failure)..."
    output=$(dbt run \
        --select "${model}" \
        --vars 'run_negative_tests: true' \
        ${PROFILE:+--profile "${PROFILE}"} 2>&1)
    code=$?
    if [ "${code}" -eq 0 ]; then
        echo "[negative-test] FAIL (${model}): built successfully but should have raised a compiler error."
        rc=1
    elif echo "${output}" | grep -q "${expected}"; then
        echo "[negative-test] PASS (${model}): failed with the expected error."
    else
        echo "[negative-test] FAIL (${model}): failed, but not with the expected message:"
        echo "${output}" | tail -5
        rc=1
    fi
done

exit "${rc}"
