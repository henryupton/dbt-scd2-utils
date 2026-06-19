{{
    config(
        materialized='incremental_scd2',
        unique_key=['customer_id', 'region']
    )
}}

{#
    Regression reproduction for null-bearing unique keys.

    `region` is part of the unique_key but is NULL for customer 1, and the
    source re-reports that key across iterations (iteration 2 carries the
    same customer 1 row with a changed status, plus an unchanged customer 2).

    Before the null-safe-key fix the incremental MERGE matched existing
    versions with per-column raw equality (DBT_INTERNAL_DEST.col =
    DBT_INTERNAL_SOURCE.col) and the previous_record lookup matched with
    p.col = n.col. NULL = NULL is UNKNOWN, so customer 1 never matched its
    already-persisted version: the prior current row was never expired and a
    fresh current row was inserted every run, accumulating duplicate current
    versions. one_current_per_key catches it.

    Run across iterations to exercise the incremental path, e.g.
    ./test_scd2_sequence.sh 1 2 null_key_scd2
#}

{#- Only two states exist (initial load, then the re-report). Clamp so the
    model still parses for any iteration value used by the shared sequence runner. -#}
{%- set iteration = var('iteration', 1) | int -%}
{%- set seed_iteration = 1 if iteration < 2 else 2 -%}

select
    customer_id,
    region,
    status,
    _updated_at::timestamp_tz as _updated_at,
    _updated_at::timestamp_tz as _created_at
from {{ ref('null_key_raw_' ~ seed_iteration) }}
