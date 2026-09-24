{#
  Content fingerprint: build guard.

  fingerprint_should_skip() answers "may this node's build be skipped?" from the ledger: true only
  when `fingerprint_skip_unchanged_upstream` is on, at least one parent is registered in this
  deploy, and every registered parent finished `unchanged` or `appended`. A parent that is
  `pending`, `new`, `modified`, `unhashable` or `error` blocks. A node with no parents, or none
  registered in this deploy, is never skipped: the guard cannot tell why it is being built.

  fingerprint_mark_skipped() records the skip so the node's own children can read it.

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
  {%- set blocking = dbt_scd2_utils.fingerprint_blocking_verdicts() | map('string') | list -%}
  {%- set res = run_query(
      "select count_if(verdict in ('" ~ (blocking | join("', '")) ~ "')) as blocking, count(*) as registered"
      ~ " from " ~ node_rel
      ~ " where deploy_id = " ~ dbt_scd2_utils.fingerprint_lit(dbt_scd2_utils.fingerprint_deploy_id())
      ~ " and node_id in (" ~ (parents | join(', ')) ~ ")").rows[0] -%}
  {%- set blocking_count = res[0] | int -%}
  {%- set registered = res[1] | int -%}
  {%- do log("fingerprint: " ~ node.name ~ " parents registered=" ~ registered ~ " blocking=" ~ blocking_count, info=true) -%}
  {{ return(registered > 0 and blocking_count == 0) }}
{% endmacro %}

{% macro fingerprint_mark_skipped(node=none, detail=none) %}
  {%- if not execute or not dbt_scd2_utils.fingerprint_enabled() -%}
    {{ return(none) }}
  {%- endif -%}
  {%- set node = node if node is not none else model -%}
  {%- do run_query(
      "update " ~ dbt_scd2_utils.fingerprint_relation('deploy_node')
      ~ " set verdict = 'skipped', detail = " ~ dbt_scd2_utils.fingerprint_lit(detail) ~ ", finished_at = current_timestamp()"
      ~ " where deploy_id = " ~ dbt_scd2_utils.fingerprint_lit(dbt_scd2_utils.fingerprint_deploy_id())
      ~ " and node_id = " ~ dbt_scd2_utils.fingerprint_lit(node.unique_id)) -%}
{% endmacro %}
