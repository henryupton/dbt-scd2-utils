{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='event_id',
        on_schema_change='sync_all_columns',
        tags=['fp_evolve']
    )
}}

{# sync_all_columns also drops columns the source stops producing, again without replacing the object. #}
select
    customer_id::varchar || '-' || to_char(_updated_at, 'YYYYMMDDHH24MISS') as event_id,
    *
from ({{ fp_customer_rows() }})
