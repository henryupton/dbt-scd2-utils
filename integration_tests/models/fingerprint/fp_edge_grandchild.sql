{{
    config(
        materialized='guarded_table',
        tags=['fp_edge']
    )
}}

{# Guarded child of a guarded child: skips when fp_edge_child_multi was skipped, builds when it was built. #}
select customer_id, status
from {{ ref('fp_edge_child_multi') }}
