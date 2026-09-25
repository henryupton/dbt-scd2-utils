{{
    config(
        materialized='overwrite_table',
        tags=['fp_edge']
    )
}}

{# Rows with a null _loaded_at. fp_edge_null_ids nulls the stamp on existing customers (rows move into the #}
{# null bucket); fp_edge_null_extra adds, changes or removes a row that only ever had a null stamp. None of #}
{# it may pass as appended. #}
{%- set null_ids = var('fp_edge_null_ids', []) -%}
select
    customer_id,
    email,
    status,
    {% if null_ids | length > 0 -%}
    case when customer_id in ({{ null_ids | join(', ') }}) then null else _loaded_at end
    {%- else -%}
    _loaded_at
    {%- endif %} as _loaded_at
from ({{ fp_customer_rows() }})
{% if var('fp_edge_null_extra', none) is not none %}
union all
select 99, 'ninety-nine@example.com', '{{ var("fp_edge_null_extra") }}', null::timestamp_tz
{% endif %}
