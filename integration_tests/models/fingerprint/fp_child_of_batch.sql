{{
    config(
        materialized='guarded_table',
        tags=['fp_edp']
    )
}}

select *
from {{ ref('fp_stg_batch') }}
