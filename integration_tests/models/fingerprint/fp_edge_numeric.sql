{{
    config(
        materialized='overwrite_table',
        tags=['fp_edge']
    )
}}

{# _loaded_at as an epoch number: the fingerprint cannot bucket it by month, so this reads unhashable. #}
select
    customer_id,
    email,
    status,
    date_part('epoch_second', _loaded_at) as _loaded_at
from ({{ fp_customer_rows() }})
