{{
    config(
        materialized='guarded_table',
        enabled=var('fp_enable_adopted', false),
        alias=var('fp_adopted_alias', 'fp_edge_adopted'),
        tags=['fp_edge']
    )
}}

{# A table that exists but was never fingerprinted (the runner builds it once with the fingerprint off, #}
{# under an alias fresh per run). With no baseline for this relation it has to build once, then skip. #}
select customer_id, status
from {{ ref('fp_edge_parent_a') }}
