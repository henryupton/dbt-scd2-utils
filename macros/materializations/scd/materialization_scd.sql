{#
  Generic Slowly Changing Dimension materialization.

  Supports SCD type 0 (insert only, original value retained), type 1 (one row
  per key, overwritten in place) and type 2 (full temporal history), selected via
  the `scd_type` config (default 2). All types emit the same audit columns
  (_is_current, _valid_from, _valid_to, _change_type) so dimension tables share a
  consistent signature regardless of type.

  All planning lives in dbt_scd2_utils.scd_plan; this block just executes the
  resulting SQL, runs post hooks, and cleans up the temp relation.

  Config:
    scd_type (int): 0, 1 or 2. Defaults to 2 (var: dbt_scd2_utils.scd_type).
    unique_key (array): Business key columns. Required.
    See README for the full set of shared audit/change configuration options.
#}

{% materialization scd, default %}

  {%- set plan = dbt_scd2_utils.scd_plan(sql) -%}

  {%- call statement('main') -%}
    {{ plan.build_sql }}
  {%- endcall -%}

  {{ run_hooks(post_hooks, inside_transaction=True) }}

  {# Drop any temp tables we've created along the way. #}
  {%- if plan.tmp_relation is not none -%}
    {%- do adapter.drop_relation(plan.tmp_relation) -%}
  {%- endif -%}

  {%- set target_relation = plan.target_relation.incorporate(type='table') -%}

  {% do persist_docs(target_relation, model) %}

  {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}
