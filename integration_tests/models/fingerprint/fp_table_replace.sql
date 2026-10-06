{{
    config(
        materialized='table',
        tags=['fingerprint']
    )
}}

{# create or replace every build: Time Travel breaks, so the verdict is always new. #}
{{ fp_customer_rows() }}
