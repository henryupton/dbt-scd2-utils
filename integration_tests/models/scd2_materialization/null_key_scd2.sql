{{
    config(
        materialized='incremental_scd2',
        unique_key=['customer_id', 'region']
    )
}}

{#
    Regression reproduction and exhaustive coverage for null-bearing unique keys.

    `region` is part of the unique_key. Three distinct entities carry a NULL in
    the key -- (1, NULL), (3, NULL) and the fully-null key (NULL, NULL) -- alongside
    a non-null control (2, US). The source re-reports these keys across three
    iterations with a mix of changes and no-op re-reports, so the incremental path
    must (a) match and expire the right prior version of each null-bearing key,
    (b) keep the three null-bearing keys distinct from one another, and (c) not
    emit a spurious version for an unchanged re-report.

    Both the incremental MERGE and the previous_record lookup must match on a
    null-safe key: NULL = NULL is UNKNOWN, so raw per-column equality leaves a
    null-bearing key's prior current row un-expired and re-inserts a fresh current
    version each run, accumulating duplicate current versions. one_current_per_key
    catches that failure mode; matches_expected_seed pins the exact full history.

    Run across iterations to exercise the incremental path, e.g.
    ./test_scd2_sequence.sh 1 3 null_key_scd2
#}

{#- Three states exist (initial load, then two re-reports). Clamp so the model
    still parses for any iteration value used by the shared sequence runner. -#}
{%- set iteration = var('iteration', 1) | int -%}
{%- set seed_iteration = iteration if 1 <= iteration <= 3 else (1 if iteration < 1 else 3) -%}

select
    customer_id,
    region,
    status,
    _updated_at::timestamp_tz as _updated_at,
    _updated_at::timestamp_tz as _created_at
from {{ ref('null_key_raw_' ~ seed_iteration) }}
