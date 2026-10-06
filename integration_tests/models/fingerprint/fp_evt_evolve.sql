{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='event_id',
        on_schema_change='append_new_columns',
        tags=['fp_evolve']
    )
}}

{# A column added here arrives by ALTER TABLE, not create or replace: the object survives and only the shape moves. #}
select
    customer_id::varchar || '-' || to_char(_updated_at, 'YYYYMMDDHH24MISS') as event_id,
    *
from ({{ fp_customer_rows() }})
