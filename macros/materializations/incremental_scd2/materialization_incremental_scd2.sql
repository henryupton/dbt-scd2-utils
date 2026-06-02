{#
  Backwards-compatible alias for the SCD type 2 materialization.

  Historically this was the package's only materialization. It now delegates to
  the shared dbt_scd2_utils.scd_plan with scd_type forced to 2, so existing
  models keep their exact behaviour. New models can instead use the generic
  `scd` materialization and pass scd_type explicitly.
#}

{% materialization incremental_scd2, default %}

  {%- set plan = dbt_scd2_utils.scd_plan(sql, scd_type=2) -%}

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
