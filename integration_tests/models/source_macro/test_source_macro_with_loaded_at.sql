-- Test 2: Source macro with loaded_at parameter but exclude_data_after_run_start = false
-- This should return the source table as-is since exclude_data_after_run_start is false by default
select 
    'with_loaded_at' as test_scenario,
    transaction_id,
    account_id,
    amount,
    transaction_date,
    loaded_at
from {{ dbt_scd2_utils.source('test_data', 'transactions', 'loaded_at') }}