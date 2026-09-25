{{
    config(
        materialized='overwrite_table',
        tags=['fp_edge'],
        pre_hook="alter session set timezone = 'Pacific/Auckland'"
    )
}}

{# timestamp_ntz loaded-at under a session twelve hours off UTC. The watermark has to be read back and #}
{# compared in the column's own type; a timestamp_tz literal would shift by the session offset and the row #}
{# fp_edge_ntz_extra adds one hour above the watermark would count as old. The session stays on Auckland #}
{# for the rest of this thread; nothing else here is sensitive to it. #}
select
    customer_id,
    email,
    status,
    convert_timezone('UTC', _loaded_at)::timestamp_ntz as _loaded_at
from ({{ fp_customer_rows() }})
{% if var('fp_edge_ntz_extra', false) %}
union all
select 90, 'ninety@example.com', 'ACTIVE', '2026-08-20 10:05:00'::timestamp_ntz
{% endif %}
