{{
    config(
        materialized='guarded_table',
        enabled=var('fp_enable_late_child', false),
        alias=var('fp_late_alias', 'fp_child_late'),
        tags=['fp_late']
    )
}}

{# Appears mid-sequence with no existing table (the runner aliases it per run): must build even #}
{# though its parent is unchanged. #}
select customer_id, email
from {{ ref('fp_dim_scd2') }}
where _is_current
