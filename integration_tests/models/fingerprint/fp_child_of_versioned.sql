{{
    config(
        materialized='guarded_table',
        tags=['fp_edp']
    )
}}

{# Refs the versioned dim by its bare name, as warehouse consumers do. #}
select customer_id, email, status
from {{ ref('fp_dim_versioned') }}
where _is_current
