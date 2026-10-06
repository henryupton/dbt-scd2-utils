{{
    config(
        materialized='view',
        tags=['fp_guard']
    )
}}

{# A view has no content of its own: always `new`, so its child always builds. #}
select *
from ({{ fp_customer_rows() }})
