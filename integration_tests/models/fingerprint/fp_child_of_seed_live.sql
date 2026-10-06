{{
    config(
        materialized='guarded_table',
        tags=['fp_edp']
    )
}}

{# Parent is a seed the runner rewrites mid-sequence: skips on an identical reload, builds on a changed one. #}
select *
from {{ ref('fp_seed_live') }}
