{#
  Test-only materialization: the insert-overwrite shape the project's truncate_insert
  uses for facts and marts. create or replace on the first build or --full-refresh, otherwise
  `insert overwrite into` the existing table by column name so the object, and its Time Travel,
  survive. Pre and post hooks run either way.
#}

{% materialization overwrite_table, default %}

  {%- set target_relation = this.incorporate(type='table') -%}
  {%- set existing_relation = load_relation(this) -%}

  {{ run_hooks(pre_hooks) }}

  {%- if existing_relation is none or not existing_relation.is_table or should_full_refresh() -%}
    {%- call statement('main') -%}
      {{ get_create_table_as_sql(False, target_relation, sql) }}
    {%- endcall -%}
  {%- else -%}
    {%- set cols_csv = dbt_scd2_utils.get_quoted_csv(adapter.get_columns_in_relation(existing_relation) | map(attribute='name') | list) -%}
    {%- call statement('main') -%}
      insert overwrite into {{ target_relation }} ({{ cols_csv }})
      select {{ cols_csv }}
      from ({{ sql }})
    {%- endcall -%}
  {%- endif -%}

  {{ run_hooks(post_hooks) }}

  {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}
