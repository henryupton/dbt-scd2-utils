{{
    config(
        materialized='scd',
        unique_key=['customer_id'],
        tags=['fingerprint'],
        meta={
            'scd_type': 1,
            'change_columns': {'exclude': ['_written_at']},
            'full_refresh_strategy': var('fp_full_refresh_strategy', 'truncate')
        }
    )
}}

select
    *,
    _updated_at as _created_at,
    sysdate()::timestamp_tz as _written_at
from ({{ fp_customer_rows() }})
