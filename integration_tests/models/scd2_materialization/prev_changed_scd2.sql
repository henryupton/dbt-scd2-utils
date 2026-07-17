{{
    config(
        materialized='incremental_scd2',
        unique_key=['customer_id'],
        meta={
            'track_previous_version': true,
            'track_changed_columns': true,
            'track_checksum': true,
            'change_columns': {
                'exclude': ['_written_at', '_created_at']
            }
        }
    )
}}

{#
    Exercises the optional _previous / _changed audit columns.

    Iteration 1 (full refresh): customer 1 has two versions (initial-load lag),
    customer 2 has one (first-version nulls).
    Iteration 2 (incremental): a backfilled row for customer 1 lands BETWEEN its
    two existing versions with genuinely different tracked columns, so the later
    existing version's _previous / _changed must be recomputed in place. This
    relies on update_all_previous_records=true (set globally for this project).
#}

{%- set iteration = var('iteration', 1) | int -%}
{%- set seed_iteration = iteration if iteration <= 2 else 2 -%}

select
    customer_id,
    customer_name,
    email,
    status,
    _created_at::timestamp_tz as _created_at,
    _updated_at::timestamp_tz as _updated_at,
    sysdate() as _written_at
from {{ ref('prev_changed_raw_' ~ seed_iteration) }}
