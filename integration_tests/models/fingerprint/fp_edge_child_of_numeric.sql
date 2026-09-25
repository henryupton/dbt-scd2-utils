{{
    config(
        materialized='guarded_table',
        tags=['fp_edge']
    )
}}

select *
from {{ ref('fp_edge_numeric') }}
