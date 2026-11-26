-- Test 5: Source macro with a load-time window
-- Run with --vars "{loaded_from: '2024-01-01 00:00:00 +00:00', loaded_to: '2024-01-02 00:00:00 +00:00'}"
-- to keep only rows loaded inside the window

select
    'loaded_window' as test_scenario,
    transaction_id,
    account_id,
    amount,
    transaction_date,
    loaded_at
from {{ dbt_scd2_utils.source('test_data', 'transactions', 'loaded_at') }}
