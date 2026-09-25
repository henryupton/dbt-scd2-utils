{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='customer_id',
        tags=['fp_edp']
    )
}}

{# The staging merge shape with a batch-stamped _loaded_at: rows arrive in batches that share one timestamp, #}
{# so a late chunk of the current batch lands exactly at the watermark and has to read modified, not appended. #}
{# fp_batch_stage: 0 empty (the table exists with no rows), 1 the first two batches, 2 a late chunk of batch #}
{# two, 3 batch three. Every run re-sends everything, so the merge rewrites matched rows with equal values. #}
with batch_rows as (
    select 1 as customer_id, 'ada@example.com' as email, 1 as stage, '2026-06-15 09:05:00+0000'::timestamp_tz as _loaded_at
    union all select 2, 'grace@example.com', 1, '2026-06-15 09:05:00+0000'::timestamp_tz
    union all select 3, 'alan@example.com', 1, '2026-07-10 09:05:00+0000'::timestamp_tz
    union all select 4, 'katherine@example.com', 2, '2026-07-10 09:05:00+0000'::timestamp_tz
    union all select 5, 'margaret@example.com', 3, '2026-08-05 09:05:00+0000'::timestamp_tz
)

select
    customer_id,
    email,
    _loaded_at
from batch_rows
where stage <= {{ var('fp_batch_stage', 0) | int }}
