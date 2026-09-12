{#
  Builds the SQL that (re)populates an SCD table on a full refresh or initial load.

  `create or replace table` drops the table object and with it everything hanging off it:
  grants, comments, tags, masking / row access policies, clustering keys and Search
  Optimization. When a model is merely being rebuilt with --full-refresh and its column set
  has not changed, that is needless churn, so this macro keeps the table and swaps only its
  rows: truncate table + insert into, wrapped in an explicit transaction. Snowflake treats
  TRUNCATE as DML, so a failed insert rolls the truncate back and the old rows survive.

  Falls back to create or replace table (the original behaviour) when:
    - there is no existing relation, or it is not a table (e.g. a view);
    - a business column was added, removed or changed type, or an audit column is missing
      from the existing table (see get_scd_schema_changes);
    - full_refresh_strategy is 'replace'.

  Config:
    full_refresh_strategy ('truncate' | 'replace'): default 'truncate'. Model meta / config,
      or the dbt_scd2_utils.full_refresh_strategy var.

  Args:
    target_relation: the model's relation.
    existing_relation: load_relation(this) at plan time (may be none).
    dest_columns: columns of the temp relation (business columns only).
    audit_columns (array): audit column names the initial load appends.
    initial_load_sql (string): the SELECT that produces the full table contents.

  Returns:
    SQL string for the materialization's main statement.
#}
{% macro get_full_refresh_sql(target_relation, existing_relation, dest_columns, audit_columns, initial_load_sql) %}

  {%- set strategy = dbt_scd2_utils.get_config_value(config, 'full_refresh_strategy', default=dbt_scd2_utils.get_from_object(var('dbt_scd2_utils', {}), 'full_refresh_strategy', default='truncate')) | string | lower -%}
  {%- if strategy not in ['truncate', 'replace'] -%}
    {{ exceptions.raise_compiler_error("full_refresh_strategy must be 'truncate' or 'replace' for " ~ target_relation ~ ", got: " ~ strategy) }}
  {%- endif -%}

  {# Any reason at all means create or replace. #}
  {%- set replace_reason = none -%}
  {%- if strategy == 'replace' -%}
    {%- set replace_reason = "full_refresh_strategy is 'replace'" -%}
  {%- elif existing_relation is none -%}
    {%- set replace_reason = 'no existing relation' -%}
  {%- elif not existing_relation.is_table -%}
    {%- set replace_reason = 'existing relation is a ' ~ existing_relation.type ~ ', not a table' -%}
  {%- else -%}
    {%- set replace_reason = dbt_scd2_utils.get_scd_schema_changes(existing_relation, dest_columns, audit_columns) -%}
    {%- if replace_reason is not none -%}
      {%- set replace_reason = 'schema changed (' ~ replace_reason ~ ')' -%}
    {%- endif -%}
  {%- endif -%}

  {%- if replace_reason is not none -%}
    {{ log("dbt_scd2_utils: full refresh of " ~ target_relation ~ " via create or replace: " ~ replace_reason, info=True) }}
    {{ return(get_create_table_as_sql(False, target_relation, initial_load_sql)) }}
  {%- endif -%}

  {{ log("dbt_scd2_utils: full refresh of " ~ target_relation ~ " via truncate + insert; schema unchanged, table metadata preserved", info=True) }}

  {# Mirror the contract check that create or replace would have run. #}
  {%- if config.get('contract').enforced -%}
    {%- do get_assert_columns_equivalent(initial_load_sql) -%}
  {%- endif -%}

  {# Select by name so the existing table's column order does not matter. #}
  {%- set insert_columns = (dest_columns | map(attribute='name') | list) + audit_columns -%}
  {%- set insert_cols_csv = dbt_scd2_utils.get_quoted_csv(insert_columns) -%}
  {%- set sql_header = config.get('sql_header', none) -%}

  {%- set build_sql -%}
{{ sql_header if sql_header is not none }}
begin;
truncate table {{ target_relation }};
insert into {{ target_relation }} ({{ insert_cols_csv }})
select {{ insert_cols_csv }}
from (
{{ initial_load_sql }}
) as dbt_scd2_utils_full_refresh;
commit;
  {%- endset -%}

  {{ return(build_sql) }}
{% endmacro %}
