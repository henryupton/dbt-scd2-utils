{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='customer_id',
        tags=['fp_edge']
    )
}}

{# The other parent. fp_edge_b_flip lowers the status, a value change the merge rewrites in place. #}
{# fp_edge_fail_b makes the build fail, so the node stays pending for the rest of its deploy. #}
select
    customer_id,
    customer_name,
    email,
    {% if var('fp_edge_b_flip', false) %}lower(status){% else %}status{% endif %} as status,
    _updated_at,
    _loaded_at
from ({{ fp_customer_rows() }})
{% if var('fp_edge_fail_b', false) %}
where to_number('boom') = 1
{% endif %}
qualify row_number() over (partition by customer_id order by _updated_at desc) = 1
