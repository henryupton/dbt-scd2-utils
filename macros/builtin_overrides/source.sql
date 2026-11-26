{#
    Override of dbt's built-in source() macro adding incremental loading and a load-time window.

    **Args:**
    - `source_name` (string): The name of the source as defined in sources.yml
    - `table_name` (string): The name of the table within the source
    - `loaded_at_col` (string, optional): Column name for incremental comparison
    - `target_loaded_at_col` (string, optional): Column in this compared against, default `_loaded_at`

    **Behavior:**
    - If running incrementally AND loaded_at_col is provided: keeps records where
      loaded_at_col > max(target_loaded_at_col) in the target table
    - If the `loaded_from` / `loaded_to` vars are set: keeps records inside that window
      (see loaded_at_bounds). The window applies to every call, using loaded_at_col or,
      when that is not passed, the source's declared loaded_at_field. A source with neither
      is read unbounded.
    - Otherwise: Returns the full source table (standard dbt behavior)

    **Example:**
    ```sql
    select * from {{ source('raw_data', 'customers', 'updated_at') }}
    ```
    On incremental runs this selects only customers with updated_at newer than the latest record
    already in the target. Run with `--vars "{loaded_to: '2026-11-25 00:00:00 +00:00'}"` and it also
    ignores anything loaded from that moment on.
#}

{% macro source(source_name, table_name, loaded_at_col=none, target_loaded_at_col='_loaded_at') -%}
  {% set source_relation = builtins.source(source_name, table_name) %}

  {% set is_incremental_run = dbt_scd2_utils.is_incremental() %}
  {% set has_loaded_at_col = loaded_at_col is not none %}

  {% set where_exprs = [] %}

  {% if is_incremental_run and has_loaded_at_col %}
      {% set default_valid_from = var('dbt_scd2_utils:default_valid_from', '1900-01-01 00:00:00') %}
      {% set where_exprs = where_exprs + ["(select coalesce(max(" ~ target_loaded_at_col ~ "), '" ~ default_valid_from ~ "'::timestamp) from " ~ this ~ ") < " ~ loaded_at_col] %}
  {% endif %}

  {% set bound_col = loaded_at_col %}
  {% if bound_col is none and execute and (var('loaded_from', none) is not none or var('loaded_to', none) is not none or var('exclude_data_after_run_start', false)) %}
      {% set source_nodes = graph.sources.values()
          | selectattr('source_name', 'equalto', source_name)
          | selectattr('name', 'equalto', table_name)
          | list %}
      {% if source_nodes %}
          {% set bound_col = source_nodes[0].get('loaded_at_field') or source_nodes[0].get('config', {}).get('loaded_at_field') %}
      {% endif %}
  {% endif %}
  {% if bound_col is not none %}
      {% set where_exprs = where_exprs + dbt_scd2_utils.loaded_at_bounds(bound_col) %}
  {% endif %}

  {% if where_exprs | length == 0 %}
    {{ return(source_relation) }}
  {% else %}
    {% set where_clause = where_exprs | join(' and ') %}
    {{ return("(select * from " ~ source_relation ~ " where " ~ where_clause ~ ")") }}
  {% endif %}

{% endmacro %}
