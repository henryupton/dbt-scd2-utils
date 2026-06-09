{{
    config(
        materialized='incremental_scd2',
        unique_key=['customer_id'],
        meta={
            'change_columns': {
                'exclude': ['_written_at', '_created_at']
            },
            'deleted_at_column': 'deleted_at'
        }
    )
}}

{#
    Regression reproduction for the out-of-order / backfill bug.

    Iteration 1 establishes a current record at _updated_at = 2024-01-10.
    Iteration 2 backfills the SAME key with an EARLIER _updated_at (2024-01-05)
    and IDENTICAL tracked columns. The earlier row's hash matches the existing
    current row, so without the fix it is removed by `changes_only`, the existing
    current row is never expired, and the backfilled row is inserted as current
    too -> two _is_current = true rows for customer_id 99.
#}

{#- Only two states exist (initial load, then the out-of-order backfill). Clamp so the
    model still parses for any iteration value used by the shared sequence runner. -#}
{%- set iteration = var('iteration', 1) | int -%}
{%- set seed_iteration = 1 if iteration < 2 else 2 -%}

select
    customer_id,
    customer_name,
    email,
    status,
    deleted_at::timestamp_tz as deleted_at,
    _created_at::timestamp_tz as _created_at,
    _updated_at,
    sysdate() as _written_at
from {{ ref('ooo_backfill_raw_' ~ seed_iteration) }}
