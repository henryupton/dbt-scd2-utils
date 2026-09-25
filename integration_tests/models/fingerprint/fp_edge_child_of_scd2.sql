{{
    config(
        materialized='guarded_table',
        tags=['fp_edge']
    )
}}

select customer_id, email
from {{ ref('fp_edge_scd2') }}
where _is_current
