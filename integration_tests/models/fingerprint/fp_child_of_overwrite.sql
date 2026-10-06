{{
    config(
        materialized='guarded_table',
        tags=['fp_overwrite']
    )
}}

select customer_id, email, status, _loaded_at
from {{ ref('fp_fct_overwrite') }}
