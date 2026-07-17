{{
    config(
        materialized='incremental_scd2',
        unique_key=['customer_id'],
        meta={'track_checksum': true}
    )
}}

{#
    Exercises the optional _checksum column for SCD type 2. Business columns are
    selected in NON-alphabetical order (status, email, customer_name, customer_id)
    on purpose: the checksum must be identical to a fixed alphabetical oracle, which
    only holds if the implementation sorts the columns before hashing.
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
