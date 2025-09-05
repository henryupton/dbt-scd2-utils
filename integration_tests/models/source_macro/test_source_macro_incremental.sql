{{
    config(
        materialized='incremental',
        unique_key='transaction_id',
        vars={
            'exclude_data_after_run_start': true
        }
    )
}}

-- Test 4: Source macro with incremental loading + exclude_data_after_run_start
-- This should load incrementally AND filter out data after run start
select 
    transaction_id,
    account_id,
    amount,
    transaction_date,
    loaded_at
from {{ dbt_scd2_utils.source('test_data', 'transactions', 'loaded_at') }}

{% if is_incremental() %}
    -- This filter will be handled by the source macro itself
{% endif %}