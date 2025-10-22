{{
    config(
        materialized='incremental_scd2',
        unique_key=['customer_id'],
        exclude_columns_from_change_check=['_written_at'],
        deleted_at_column='deleted_at'
    )
}}

{%- set iteration = var('iteration', 1) -%}

select
    customer_id,
    customer_name,
    email,
    status,
    deleted_at::timestamp_tz as deleted_at,
    _updated_at,
    sysdate() as _written_at
from {{ ref('customers_raw_' ~ iteration) }}