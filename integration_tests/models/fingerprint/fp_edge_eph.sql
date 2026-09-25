{{
    config(
        materialized='ephemeral',
        tags=['fp_edge']
    )
}}

{# Ephemeral: no relation, no hooks, no verdict. The register leaves it out and the guard looks #}
{# through it to fp_edge_parent_a. #}
select customer_id, email
from {{ ref('fp_edge_parent_a') }}
