{{
    config(
        materialized='incremental_scd2',
        unique_key=['customer_id'],
        exclude_columns_from_change_check=['_written_at']
    )
}}

{%- set iteration = var('iteration', 1) -%}

select 
    customer_id,
    customer_name,
    email,
    status,
    _updated_at,
    sysdate() as _written_at
from {{ ref('customers_raw_' ~ iteration) }}