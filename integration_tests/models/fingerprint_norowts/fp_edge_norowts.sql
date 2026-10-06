{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='customer_id',
        tags=['fp_edge']
    )
}}

{# This folder's post-hooks switch row timestamps off before the fingerprint reads the table, so #}
{# METADATA$ROW_LAST_COMMIT_TIME is an invalid identifier here: the verdict must be unhashable, not a hook error. #}
select customer_id, email, status, _loaded_at
from ({{ fp_customer_rows() }})
qualify row_number() over (partition by customer_id order by _updated_at desc) = 1
