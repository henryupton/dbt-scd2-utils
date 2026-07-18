{{
    config(
        materialized='incremental_scd2',
        unique_key=['customer_id'],
        meta={
            'search_optimization': true,
            'assume_keys_not_null': true
        }
    )
}}

{#
    Exercises the plain `=` key match (assume_keys_not_null=true, no null guard) plus the opt-in
    Search Optimization Service. customer_id is a non-null single-column key. On a full refresh the
    materialization should ADD SEARCH OPTIMIZATION ON EQUALITY(customer_id); on incremental runs it
    should not re-issue the ALTER. Correctness is checked by the shared SCD2 invariant tests.
#}

{%- set iteration = var('iteration', 1) | int -%}
{%- set seed_iteration = iteration if 1 <= iteration <= 6 else (1 if iteration < 1 else 6) -%}

select
    customer_id,
    customer_name,
    email,
    status,
    _updated_at::timestamp_tz as _updated_at,
    _updated_at::timestamp_tz as _created_at
from {{ ref('customers_raw_' ~ seed_iteration) }}
