{{
    config(
        materialized='scd',
        unique_key=['customer_id'],
        tags=['fp_edge'],
        meta={
            'scd_type': 2,
            'full_refresh_strategy': var('fp_full_refresh_strategy', 'truncate')
        }
    )
}}

{# SCD type 2 for the validity blind spot: a full refresh with a different default_valid_to moves _valid_to #}
{# on every current row and nothing else. Excluded (the default) that reads unchanged; included it reads modified. #}
select
    customer_id,
    customer_name,
    email,
    status,
    _updated_at,
    _updated_at as _created_at,
    _loaded_at
from ({{ fp_customer_rows() }})
