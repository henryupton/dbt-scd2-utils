{#
  Content fingerprint: build guard.

  fingerprint_should_skip() answers "may this node's build be skipped?" from the ledger: true only
  when `fingerprint_skip_unchanged_upstream` is on, at least one parent is registered in this
  deploy, every registered parent finished `unchanged`, `appended` or `skipped`, and the node's
  own source checksum matches the one recorded at its last fingerprinted build. A parent that is
  `pending`, `new`, `modified`, `unhashable` or `error` blocks. A node with no parents, none
  registered in this deploy, or no fingerprinted build on record is never skipped: the guard
  cannot tell why it is being built. Nor is a node whose own SQL changed, however its parents
  came out; the parents say nothing about that.

  fingerprint_mark_skipped() appends the skip so the node's own children can read it.

  These are plain macros for a materialization to call; nothing in this package calls them.
#}

{% macro fingerprint_should_skip(node=none) %}
  {%- if not execute or not dbt_scd2_utils.fingerprint_enabled() or not dbt_scd2_utils.fingerprint_skip_enabled() -%}
    {{ return(false) }}
  {%- endif -%}
  {%- set node = node if node is not none else model -%}
  {%- set parents = [] -%}
  {%- for p in node.depends_on.nodes -%}
    {%- if p.startswith('model.') or p.startswith('seed.') or p.startswith('snapshot.') -%}
      {%- do parents.append(dbt_scd2_utils.fingerprint_lit(p)) -%}
    {%- endif -%}
  {%- endfor -%}
  {%- if parents | length == 0 -%}
    {{ return(false) }}
  {%- endif -%}

  {%- set node_rel = dbt_scd2_utils.fingerprint_relation('deploy_node') -%}
  {%- set deploy_lit = dbt_scd2_utils.fingerprint_lit(dbt_scd2_utils.fingerprint_deploy_id()) -%}
  {%- set blocking = dbt_scd2_utils.fingerprint_blocking_verdicts() | map('string') | list -%}
  {%- set res = run_query(
      "with latest as (select node_id, verdict from " ~ node_rel
      ~ " where deploy_id = " ~ deploy_lit ~ " and node_id in (" ~ (parents | join(', ')) ~ ")"
      ~ dbt_scd2_utils.fingerprint_latest_row() ~ ")"
      ~ " select count_if(verdict in ('" ~ (blocking | join("', '")) ~ "')) as blocking, count(*) as registered from latest").rows[0] -%}
  {%- set blocking_count = res[0] | int -%}
  {%- set registered = res[1] | int -%}
  {%- if registered == 0 or blocking_count > 0 -%}
    {%- do log("fingerprint: " ~ node.name ~ " parents registered=" ~ registered ~ " blocking=" ~ blocking_count ~ "; building", info=true) -%}
    {{ return(false) }}
  {%- endif -%}

  {# The node's own SQL: its checksum has to match the one recorded at its last fingerprinted build. #}
  {%- set current = dbt_scd2_utils.fingerprint_node_checksum(node) -%}
  {%- set base = run_query(
      "select checksum, deploy_id from " ~ node_rel
      ~ " where node_id = " ~ dbt_scd2_utils.fingerprint_lit(node.unique_id) ~ " and verdict <> 'pending'"
      ~ " order by finished_at desc nulls last, registered_at desc limit 1") -%}
  {%- if base.rows | length == 0 -%}
    {%- do log("fingerprint: " ~ node.name ~ " has no fingerprinted build on record; building", info=true) -%}
    {{ return(false) }}
  {%- endif -%}
  {%- set baseline = base.rows[0][0] -%}
  {%- if current is none and baseline is none -%}
    {%- do log("fingerprint: " ~ node.name ~ " has no checksum on either side; skipping on parent verdicts alone", info=true) -%}
    {{ return(true) }}
  {%- elif current != baseline -%}
    {%- do log("fingerprint: " ~ node.name ~ " checksum differs from its last build in deploy " ~ base.rows[0][1] ~ "; building", info=true) -%}
    {{ return(false) }}
  {%- endif -%}
  {%- do log("fingerprint: " ~ node.name ~ " parents registered=" ~ registered ~ " blocking=0, checksum unchanged; may skip", info=true) -%}
  {{ return(true) }}
{% endmacro %}

{% macro fingerprint_mark_skipped(node=none, detail=none) %}
  {%- if not execute or not dbt_scd2_utils.fingerprint_enabled() -%}
    {{ return(none) }}
  {%- endif -%}
  {%- set node = node if node is not none else model -%}
  {%- set node_rel = dbt_scd2_utils.fingerprint_relation('deploy_node') -%}
  {%- set key = " where deploy_id = " ~ dbt_scd2_utils.fingerprint_lit(dbt_scd2_utils.fingerprint_deploy_id())
      ~ " and node_id = " ~ dbt_scd2_utils.fingerprint_lit(node.unique_id) -%}
  {%- do run_query(
      "insert into " ~ node_rel
      ~ " (deploy_id, node_id, node_name, relation_name, parents, snapshot_at, loaded_at_column, verdict, detail, registered_at, finished_at, pre_shape, checksum)"
      ~ " select deploy_id, node_id, node_name, relation_name, parents, snapshot_at, loaded_at_column,"
      ~ " 'skipped', " ~ dbt_scd2_utils.fingerprint_lit(detail) ~ ", registered_at, current_timestamp(), pre_shape, checksum"
      ~ " from " ~ node_rel ~ key ~ " and verdict = 'pending'") -%}
{% endmacro %}
