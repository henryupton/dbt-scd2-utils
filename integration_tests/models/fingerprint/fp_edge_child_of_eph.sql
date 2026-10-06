{{
    config(
        materialized='guarded_table',
        tags=['fp_edge']
    )
}}

{# Its only parent is ephemeral; it skips when fp_edge_parent_a is unchanged and builds when that flips. #}
select *
from {{ ref('fp_edge_eph') }}
