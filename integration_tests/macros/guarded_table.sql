{#
  Test-only materialization: a plain table build that asks the fingerprint guard
  first. When every registered parent finished unchanged or appended it leaves the existing
  table alone and records `skipped`; otherwise it builds with create or replace. Exists to
  exercise fingerprint_should_skip without touching the package materializations.
#}

{% materialization guarded_table, default %}

  {%- set target_relation = this.incorporate(type='table') -%}
  {%- set existing_relation = load_relation(this) -%}

  {%- if existing_relation is not none and existing_relation.is_table and dbt_scd2_utils.fingerprint_should_skip() -%}
    {%- do dbt_scd2_utils.fingerprint_mark_skipped(detail='no blocking parent verdict') -%}
    {%- do log("guarded_table: skipping " ~ target_relation ~ ", upstream unchanged", info=true) -%}
    {%- call statement('main') -%}
      select 'skipped' as outcome
    {%- endcall -%}
    {{ return({'relations': [existing_relation]}) }}
  {%- endif -%}

  {{ run_hooks(pre_hooks) }}

  {%- call statement('main') -%}
    {{ get_create_table_as_sql(False, target_relation, sql) }}
  {%- endcall -%}

  {{ run_hooks(post_hooks) }}

  {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}
