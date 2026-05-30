#!/usr/bin/env bash
#
# Negative test for the scd materialization.
#
# Setting deleted_at_column on an SCD type 0 or 1 model must raise a compiler
# error. The offending model (customers_scd1_deleted_at_invalid) is disabled by
# default via enabled=var('run_negative_tests', false), so it never affects normal
# runs. This script enables it and asserts that the run fails with the expected
# error.
#
# Usage:
#   ./test_scd_negative.sh [profile]
#   ./test_scd_negative.sh integration_tests

set -uo pipefail
cd "$(dirname "$0")"

PROFILE="${1:-}"
EXPECTED="deletion tracking is not supported for SCD types 0 and 1"
MODEL="customers_scd1_deleted_at_invalid"

echo "[negative-test] Running ${MODEL} (expecting failure)..."
output=$(dbt run \
    --select "${MODEL}" \
    --vars 'run_negative_tests: true' \
    ${PROFILE:+--profile "${PROFILE}"} 2>&1)
code=$?
echo "${output}"
echo "----------------------------------------"

if [ "${code}" -eq 0 ]; then
    echo "[negative-test] FAIL: model built successfully but should have raised a compiler error."
    exit 1
fi

if echo "${output}" | grep -q "${EXPECTED}"; then
    echo "[negative-test] PASS: model failed with the expected deleted_at error."
    exit 0
fi

echo "[negative-test] FAIL: model failed, but not with the expected error message."
exit 1
