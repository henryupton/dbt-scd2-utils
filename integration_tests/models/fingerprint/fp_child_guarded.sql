{{
    config(
        materialized='guarded_table',
        tags=['fingerprint']
    )
}}

select
    customer_id,
    customer_name,
    email,
    status,
    _loaded_at
from {{ ref('fp_dim_scd2') }}
where _is_current
