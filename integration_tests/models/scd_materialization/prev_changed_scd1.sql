{{
    config(
        materialized='scd',
        unique_key=['customer_id'],
        meta={
            'scd_type': 1,
            'track_previous_version': true,
            'track_changed_columns': true,
            'change_columns': {
                'exclude': ['_written_at', '_created_at']
            }
        }
    )
}}

{#
    track_previous_version / track_changed_columns are SCD2-only. On this type-1
    model they must emit a warning and be ignored (no _PREVIOUS / _CHANGED columns),
    while the model still builds normally. Reuses the prev_changed_raw_1 seed.
#}

select
    customer_id,
    customer_name,
    email,
    status,
    _created_at::timestamp_tz as _created_at,
    _updated_at::timestamp_tz as _updated_at,
    sysdate() as _written_at
from {{ ref('prev_changed_raw_1') }}
