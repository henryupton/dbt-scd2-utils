{{
    config(
        materialized='scd',
        unique_key=['customer_id'],
        tags=['fp_edp'],
        meta={
            'scd_type': 2,
            'change_columns': {'exclude': ['_written_at']},
            'full_refresh_strategy': var('fp_full_refresh_strategy', 'truncate')
        }
    )
}}

{# The warehouse dim shape: a versioned model. The relation is fp_dim_versioned_v1; Fusion may add a #}
{# latest-version pointer view at the bare name, which is not a node and is never fingerprinted. #}
{# The child refs the bare name and its depends_on carries the versioned unique_id. #}
select
    *,
    _updated_at as _created_at,
    sysdate()::timestamp_tz as _written_at
from ({{ fp_customer_rows() }})
