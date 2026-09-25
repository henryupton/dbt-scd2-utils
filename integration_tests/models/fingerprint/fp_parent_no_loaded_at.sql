{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='customer_id',
        tags=['fp_guard']
    )
}}

{# No loaded-at column: the fingerprint must call this unhashable and its child must build. #}
select customer_id, customer_name, status
from ({{ fp_customer_rows() }})
qualify row_number() over (partition by customer_id order by _updated_at desc) = 1
