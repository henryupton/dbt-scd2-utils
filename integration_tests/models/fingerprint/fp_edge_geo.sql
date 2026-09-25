{{
    config(
        materialized='overwrite_table',
        tags=['fp_edge']
    )
}}

{# A GEOGRAPHY column hash_agg rejects (left out, named in the detail) beside an OBJECT and an ARRAY it #}
{# must hash: fp_edge_geo_tier changes a value inside the object and has to read modified. #}
select
    customer_id,
    email,
    st_makepoint(customer_id * 1.5, customer_id * 0.5) as location,
    object_construct('status', status, 'tier', '{{ var("fp_edge_geo_tier", "standard") }}') as attrs,
    array_construct(customer_id, status) as tags_arr,
    _loaded_at
from ({{ fp_customer_rows() }})
