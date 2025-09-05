{# This model tests the source() macro override with incremental loading #}
select 
    order_id,
    customer_id,
    order_date,
    loaded_at
from {{ dbt_scd2_utils.source('test_data', 'raw_orders', 'loaded_at') }}