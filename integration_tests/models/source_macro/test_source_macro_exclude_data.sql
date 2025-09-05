-- Test 3: Source macro with loaded_at parameter and exclude_data_after_run_start = true
-- This should filter out data that arrived after the run started
-- Note: Run with --vars "exclude_data_after_run_start: true" to see the filtering effect

select 
    'exclude_after_run_start' as test_scenario,
    transaction_id,
    account_id,
    amount,
    transaction_date,
    loaded_at
from {{ dbt_scd2_utils.source('test_data', 'transactions', 'loaded_at') }}