{{
    config(
        materialized='incremental_scd2',
        unique_key=['customer_id'],
        scd_check_columns=['customer_name', 'email', 'status']
    )
}}

{# This model tests the is_incremental() macro override behavior #}
select 
    customer_id,
    customer_name,
    email,
    status,
    _updated_at
from {{ ref('unit_test_customers_input') }}

{# Add incremental logic to test the macro override #}
{% if is_incremental() %}
  {# This should only execute when is_incremental() returns true #}
  where _updated_at > (select max(_updated_at) from {{ this }})
{% endif %}