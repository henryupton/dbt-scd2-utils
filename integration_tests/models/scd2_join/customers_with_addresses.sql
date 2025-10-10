{{
    config(
        materialized='table'
    )
}}

{{ dbt_scd2_utils.scd2_join(
    [ref('customers_join_scd2'), ref('addresses_join_scd2'), ref('credit_ratings_join_scd2')],
    ['customer_id']
) }}
