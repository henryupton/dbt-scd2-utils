{{
    config(
        materialized='scd',
        unique_key=['customer_id'],
        meta={'scd_type': 1, 'track_checksum': true}
    )
}}

{#
    SCD type 1 _checksum: recomputed when a key's business columns are overwritten in
    place. Iteration 2 overwrites customer 1 with new content, so its _checksum must
    change to match (not stay stale). Non-alphabetical select order exercises the sort.
#}

{%- set iteration = var('iteration', 1) | int -%}
{%- set seed_iteration = iteration if iteration <= 2 else 2 -%}

select
    status,
    email,
    customer_name,
    customer_id,
    _updated_at::timestamp_tz as _created_at,
    _updated_at::timestamp_tz as _updated_at
from {{ ref('checksum_raw_' ~ seed_iteration) }}
