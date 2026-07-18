{#
  Adds Snowflake Search Optimization (EQUALITY method) on the given columns of a relation.

  Called from the materialization AFTER the target is (re)created, and only on a full refresh
  / initial load: `create or replace table` drops search optimization, so it is re-added on the
  create, and it then persists (auto-maintained) across incremental merges with no per-run work.
  Enabling search_optimization on an existing model therefore takes effect on the next
  `--full-refresh`. This gate avoids re-adding an existing path (which errors) and needs no
  state lookup.

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
