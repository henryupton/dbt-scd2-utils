{#
  Negative test fixture: an unsupported scd_type must raise a compiler error.
  Disabled by default so it never affects normal runs; enable it with
  --vars 'run_negative_tests: true' (see test_scd_negative.sh), which is expected
  to FAIL with the "scd_type must be 0, 1 or 2" error.
#}
{{
    config(
        enabled=var('run_negative_tests', false),
        materialized='scd',
        unique_key=['customer_id'],
        meta={
            'scd_type': 3
        }
    )
}}

select
    customer_id,
    _updated_at
from {{ ref('customers_raw_1') }}
