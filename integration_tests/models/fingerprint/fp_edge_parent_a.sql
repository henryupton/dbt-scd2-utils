{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='customer_id',
        tags=['fp_edge']
    )
}}

{# One of two parents. fp_edge_a_flip lowers the email, a value change below the watermark that the #}
{# unguarded merge rewrites in place. The ephemeral fp_edge_eph reads this table and nothing else. #}
select
    customer_id,
    customer_name,
    {% if var('fp_edge_a_flip', false) %}lower(email){% else %}email{% endif %} as email,
    status,
    _updated_at,
    _loaded_at
from ({{ fp_customer_rows() }})
qualify row_number() over (partition by customer_id order by _updated_at desc) = 1
