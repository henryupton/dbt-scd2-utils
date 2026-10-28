{{
    config(
        materialized='incremental_scd2',
        unique_key=['customer_id'],
        meta={
            'change_columns': {
                'exclude': ['_written_at', '_created_at', '_loaded_at']
            },
            'deleted_at_column': 'deleted_at',
            'restate_versions': true
        }
    )
}}

{#
    restate_versions: the source is re-derived, so a later load can carry a corrected value for a
    version that is already persisted. The incoming row must replace it, and the incremental run
    must land exactly what a full refresh over the same input would.

    Iteration 1 loads restated_history_raw_1. Iteration 2 reads restated_history_raw_2, where:
      500  a late value cuts a version at 02-01 and restates the persisted 03-01 version
      501  the restated 03-01 version now matches the new 02-01 cut and must be deleted
      502  the persisted 02-01 version is corrected in place
      503  append-style redelivery of identical content: the earliest load still survives
      504  untouched control
      505  the first version is corrected and must stay 'I'
    restated_history_expected_2 is also the full-refresh result over raw_2.
    Run via ./test_scd2_sequence.sh 1 2 restated_history_scd2
#}

{%- set iteration = var('iteration', 1) | int -%}
{%- set seed_iteration = 1 if iteration < 2 else 2 -%}

select
    customer_id,
    customer_name,
    email,
    status,
    deleted_at::timestamp_tz as deleted_at,
    _created_at::timestamp_tz as _created_at,
    _updated_at::timestamp_tz as _updated_at,
    _loaded_at::timestamp_tz as _loaded_at,
    sysdate() as _written_at
from {{ ref('restated_history_raw_' ~ seed_iteration) }}
