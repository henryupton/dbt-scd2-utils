{{
    config(
        materialized='incremental_scd2',
        unique_key=['customer_id']
    )
}}

select
    customer_id,
    city,
    state,
    _updated_at
from {{ ref('addresses_source') }}