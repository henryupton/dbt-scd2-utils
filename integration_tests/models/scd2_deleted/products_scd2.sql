{{
    config(
        materialized='incremental_scd2',
        unique_key=['product_id'],
        deleted_at_column='deleted_at'
    )
}}

select
    product_id,
    product_name,
    price,
    deleted_at,
    _updated_at
from {{ ref('products_source') }}
