{{
    config(
        materialized='incremental',
        incremental_strategy='append',
        full_refresh=var('fp_allow_stg_full_refresh', false),
        pre_hook="{{ fp_delete_customer_hook() }}",
        tags=['fingerprint']
    )
}}

{# Protected firehose shape: append above the watermark, never full-refreshed. A pre-hook delete #}
{# here is a deletion in a month the append never touches, so only the per-month counts can see it. #}
select *
from ({{ fp_customer_rows() }})
{% if is_incremental() %}
where _loaded_at > (select max(_loaded_at) from {{ this }})
{% endif %}
