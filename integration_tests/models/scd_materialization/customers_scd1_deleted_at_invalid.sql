{#
  Negative test fixture: deleted_at_column is not supported for SCD types 0 and 1
  and must raise a compiler error. Disabled by default so it never affects normal
  runs; enable it with --vars 'run_negative_tests: true' (see test_scd_negative.sh),
  which is expected to FAIL with the deleted_at error.
#}
{{
    config(
        enabled=var('run_negative_tests', false),
        materialized='scd',
        unique_key=['customer_id'],
        meta={
            'scd_type': 1,
            'deleted_at_column': 'deleted_at'
        }
    )
}}

select
    customer_id,
    deleted_at,
    _updated_at
from {{ ref('customers_raw_1') }}
