{{
    config(
        materialized='table'
    )
}}

{{ dbt_scd2_utils.scd2_join(
    [ref('orders_join_scd2'), ref('order_items_join_scd2')],
    ['customer_id', 'order_id']
) }}