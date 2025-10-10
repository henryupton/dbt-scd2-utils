{{
    config(
        materialized='incremental_scd2',
        unique_key=['customer_id']
    )
}}

select
    customer_id,
    customer_name,
    email,
    _updated_at
from {{ ref('customers_source') }}