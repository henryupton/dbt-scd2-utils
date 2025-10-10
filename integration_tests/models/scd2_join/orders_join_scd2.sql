{{
    config(
        materialized='incremental_scd2',
        unique_key=['customer_id', 'order_id']
    )
}}

select
    customer_id,
    order_id,
    order_status,
    _updated_at
from {{ ref('orders_source') }}