{{
    config(
        materialized='overwrite_table',
        tags=['fp_overwrite']
    )
}}

{# The insert-overwrite fact shape: every row rewritten, object and Time Travel kept. #}
{{ fp_customer_rows() }}
