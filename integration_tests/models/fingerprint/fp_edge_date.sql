{{
    config(
        materialized='overwrite_table',
        tags=['fp_edge']
    )
}}

{# date loaded-at. fp_edge_date_extra lists dates for extra rows: one on the watermark day is a row at, #}
{# not above, the watermark and reads modified; a later day reads appended. #}
select
    customer_id,
    email,
    status,
    _loaded_at::date as _loaded_at
from ({{ fp_customer_rows() }})
{% for d in var('fp_edge_date_extra', []) %}
union all
select {{ 90 + loop.index }}, 'extra{{ loop.index }}@example.com', 'ACTIVE', '{{ d }}'::date
{% endfor %}
