{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='event_id',
        pre_hook="{{ fp_delete_customer_hook() }}",
        tags=['fingerprint']
    )
}}

{# dbt's merge rewrites every matched row (update set *), so old rows are touched but equal. #}
select
    customer_id::varchar || '-' || to_char(_updated_at, 'YYYYMMDDHH24MISS') as event_id,
    *
from ({{ fp_customer_rows() }})
