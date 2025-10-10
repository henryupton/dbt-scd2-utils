{{
    config(
        materialized='incremental_scd2',
        unique_key=['customer_id', 'order_id']
    )
}}

select
    customer_id,
    order_id,
    item_name,
    quantity,
    _updated_at
from {{ ref('order_items_source') }}