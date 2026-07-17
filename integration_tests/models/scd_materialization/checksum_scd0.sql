{{
    config(
        materialized='scd',
        unique_key=['customer_id'],
        meta={'scd_type': 0, 'track_checksum': true}
    )
}}

{#
    SCD type 0 _checksum: computed once for the retained (earliest) row, never updated.
    Non-alphabetical select order to exercise the checksum sort.
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
