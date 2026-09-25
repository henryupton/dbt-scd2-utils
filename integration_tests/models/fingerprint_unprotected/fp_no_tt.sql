{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='customer_id',
        tags=['fp_guard']
    )
}}

{# Lives in the folder without the retention hook, so it keeps retention 0 and has no Time Travel. #}
select *
from ({{ fp_customer_rows() }})
qualify row_number() over (partition by customer_id order by _updated_at desc) = 1
