{{
    config(
        materialized='guarded_table',
        tags=['fp_guard']
    )
}}

select *
from {{ ref('fp_parent_view') }}
