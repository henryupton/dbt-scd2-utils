{{
    config(
        materialized='guarded_table',
        tags=['fp_evolve']
    )
}}

select *
from {{ ref('fp_evt_sync') }}
