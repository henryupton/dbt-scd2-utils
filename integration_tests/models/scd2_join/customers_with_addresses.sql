{{
    config(
        materialized='table'
    )
}}

{{ dbt_scd2_utils.scd2_join(
    [ref('customers_scd2'), ref('addresses_scd2')],
    'customer_id'
) }}