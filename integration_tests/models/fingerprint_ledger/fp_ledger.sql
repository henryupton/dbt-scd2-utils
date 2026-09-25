{{
    config(
        materialized='view',
        tags=['fingerprint', 'fp_overwrite', 'fp_evolve', 'fp_guard', 'fp_late', 'fp_edp', 'fp_edge']
    )
}}

-- depends_on: {{ ref('fp_dim_scd2') }}
-- depends_on: {{ ref('fp_dim_scd1') }}
-- depends_on: {{ ref('fp_evt_merge') }}
-- depends_on: {{ ref('fp_stg_append') }}
-- depends_on: {{ ref('fp_table_replace') }}
-- depends_on: {{ ref('fp_child_guarded') }}
-- depends_on: {{ ref('fp_fct_overwrite') }}
-- depends_on: {{ ref('fp_child_of_overwrite') }}
-- depends_on: {{ ref('fp_evt_evolve') }}
-- depends_on: {{ ref('fp_child_of_evolve') }}
-- depends_on: {{ ref('fp_evt_sync') }}
-- depends_on: {{ ref('fp_child_of_sync') }}
-- depends_on: {{ ref('fp_parent_no_loaded_at') }}
-- depends_on: {{ ref('fp_child_of_unhashable') }}
-- depends_on: {{ ref('fp_parent_view') }}
-- depends_on: {{ ref('fp_child_of_view') }}
-- depends_on: {{ ref('fp_child_of_seed') }}
-- depends_on: {{ ref('fp_no_tt') }}
-- depends_on: {{ ref('fp_child_of_no_tt') }}
-- depends_on: {{ ref('fp_dim_versioned') }}
-- depends_on: {{ ref('fp_child_of_versioned') }}
-- depends_on: {{ ref('fp_stg_batch') }}
-- depends_on: {{ ref('fp_child_of_batch') }}
-- depends_on: {{ ref('fp_child_of_registry') }}
-- depends_on: {{ ref('fp_child_of_seed_live') }}
-- depends_on: {{ ref('fp_edge_parent_a') }}
-- depends_on: {{ ref('fp_edge_parent_b') }}
-- depends_on: {{ ref('fp_edge_child_of_eph') }}
-- depends_on: {{ ref('fp_edge_child_multi') }}
-- depends_on: {{ ref('fp_edge_grandchild') }}
-- depends_on: {{ ref('fp_edge_scd2') }}
-- depends_on: {{ ref('fp_edge_child_of_scd2') }}
-- depends_on: {{ ref('fp_edge_norowts') }}
-- depends_on: {{ ref('fp_edge_child_of_norowts') }}
-- depends_on: {{ ref('fp_edge_numeric') }}
-- depends_on: {{ ref('fp_edge_child_of_numeric') }}
-- depends_on: {{ ref('fp_edge_geo') }}
-- depends_on: {{ ref('fp_edge_quoted') }}
-- depends_on: {{ ref('fp_edge_nulls') }}
-- depends_on: {{ ref('fp_edge_ntz') }}
-- depends_on: {{ ref('fp_edge_date') }}
{% if var('fp_enable_late_child', false) %}
-- depends_on: {{ ref('fp_child_late') }}
{% endif %}
{% if var('fp_enable_adopted', false) %}
-- depends_on: {{ ref('fp_edge_adopted') }}
{% endif %}

{# The ledger is append-only: one pending row per node, then the verdict row. Latest wins. #}
select
    node_name,
    verdict,
    detail,
    replaced,
    pre_rows,
    post_rows,
    watermark,
    appended_rows,
    touched_rows,
    touched_old_rows,
    buckets_hashed,
    buckets_changed,
    pre_shape,
    post_shape,
    checksum,
    snapshot_at,
    finished_at
from {{ dbt_scd2_utils.fingerprint_relation('deploy_node') }}
where deploy_id = '{{ var("deploy_id", invocation_id) }}'
  and node_id <> '{{ model.unique_id }}'
{{ dbt_scd2_utils.fingerprint_latest_row() }}
