{#
  Adds Snowflake Search Optimization (EQUALITY method) on the given columns of a relation.

  Called from the materialization AFTER the main statement, on the full-refresh / initial-load
  path only. `create or replace table` drops search optimization; truncate + insert (see
  get_full_refresh_sql) and incremental merges keep it, auto-maintained with no per-run work.
  Snowflake treats ADD SEARCH OPTIMIZATION as additive and re-adding an existing EQUALITY column
  is a silent no-op, so the ALTER is simply re-asserted on every full refresh whichever path ran,
  with no state lookup. Enabling search_optimization on an existing model therefore takes effect
  on the next `--full-refresh`. Because ADD is additive, removing a column from
  search_optimization_columns does not drop its path: run one `--full-refresh` with
  full_refresh_strategy='replace', or ALTER TABLE ... DROP SEARCH OPTIMIZATION by hand.

  Requires Snowflake Enterprise Edition and incurs ongoing storage + serverless maintenance
  cost, so the caller only invokes this when the user has opted in via `search_optimization`.

  Args:
    relation: the target relation to alter.
    columns (array): columns to build EQUALITY search optimization on.
#}
{%- macro add_search_optimization(relation, columns) -%}
  {%- if columns is none or (columns | length) == 0 -%}
    {{ return(none) }}
  {%- endif -%}

  {%- set cols_csv = dbt_scd2_utils.get_quoted_csv(columns | map('upper')) -%}
  {%- call statement('dbt_scd2_utils_add_search_optimization') -%}
    alter table {{ relation }} add search optimization on equality({{ cols_csv }})
  {%- endcall -%}
  {{ log("dbt_scd2_utils: added search optimization on equality(" ~ (columns | join(', ')) ~ ") to " ~ relation, info=True) }}
{%- endmacro -%}
