{{
    config(
        materialized='guarded_table',
        tags=['fp_guard']
    )
}}

{# Parent is a seed: unregistered when not selected (must build), `new` when it is (must build). #}
select *
from {{ ref('fp_raw_deleted_b') }}
