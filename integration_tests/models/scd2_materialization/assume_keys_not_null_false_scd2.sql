{{
    config(
        materialized='incremental_scd2',
        unique_key=['customer_id', 'region'],
        meta={'assume_keys_not_null': false}
    )
}}

{#
    Explicit opt-out: assume_keys_not_null=false forces null-safe equal_null matching with NO runtime
    null guard and NO warning, even though `region` is null-bearing. Reuses the null_key fixtures, so
    the output must match the same null_key_expected_* golden snapshots as the default (guard
    fallback) path, proving the explicit and inferred equal_null paths are equivalent.
#}

{%- set iteration = var('iteration', 1) | int -%}
{%- set seed_iteration = iteration if 1 <= iteration <= 3 else (1 if iteration < 1 else 3) -%}

select
    customer_id,
    region,
    status,
    _updated_at::timestamp_tz as _updated_at,
    _updated_at::timestamp_tz as _created_at
from {{ ref('null_key_raw_' ~ seed_iteration) }}
