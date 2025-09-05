-- Test 1: Basic source macro call without loaded_at parameter
-- This should return the source table as-is
select 
    'basic' as test_scenario,
    transaction_id,
    account_id,
    amount,
    transaction_date,
    loaded_at
from {{ dbt_scd2_utils.source('test_data', 'transactions') }}