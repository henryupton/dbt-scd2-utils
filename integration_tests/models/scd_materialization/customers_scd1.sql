{{
    config(
        materialized='scd',
        unique_key=['customer_id'],
        meta={
            'scd_type': 1
        }
    )
}}

{%- set iteration = var('iteration', 1) -%}

select
    customer_id,
    customer_name,
    email,
    status,
    _updated_at,
    _updated_at as _created_at
from {{ ref('customers_raw_' ~ iteration) }}
