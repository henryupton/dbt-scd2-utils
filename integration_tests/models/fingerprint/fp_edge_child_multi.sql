{{
    config(
        materialized='guarded_table',
        tags=['fp_edge']
    )
}}

{# Two parents: one blocking verdict on either side builds it. The runner rewrites this file in one #}
{# scenario so its checksum moves while both parents stay unchanged. #}
select a.customer_id, a.email, b.status
from {{ ref('fp_edge_parent_a') }} a
join {{ ref('fp_edge_parent_b') }} b on a.customer_id = b.customer_id
