{%- docs source -%}
Override of dbt's built-in source() macro to add incremental loading capability.

This enhanced source macro adds optional incremental loading by comparing a
loaded_at timestamp column against the maximum value in the target table.
It also supports excluding data that arrived after the dbt run started to 
maintain consistency across all tables in a run.

**Args:**
- `source_name` (string): The name of the source as defined in sources.yml
- `table_name` (string): The name of the table within the source
- `loaded_at_col` (string, optional): Column name for incremental comparison

**Returns:**
- SQL that selects from the source table, optionally filtered for incremental loads

**Behavior:**
- If running incrementally AND loaded_at_col is provided: Filters for records
  where loaded_at_col > max(loaded_at_col) from target table
- If exclude_data_after_run_start is true: Also filters out records where 
  loaded_at_col > run start time
- Otherwise: Returns the full source table (standard dbt behavior)

**Example:**
```sql
select * from {{ source('raw_data', 'customers', 'updated_at') }}
```
On incremental runs, this will only select customers with updated_at timestamps
newer than the latest record already in the target table. If exclude_data_after_run_start
is enabled, it will also exclude any data that arrived after the run started.
{%- enddocs -%}

{% macro source(source_name, table_name, loaded_at_col=none) -%}
  {% set source_relation = builtins.source(source_name, table_name) %}

  {% set exclude_data = var('exclude_data_after_run_start', false) %}
  {% set is_incremental_run = dbt_scd2_utils.is_incremental() %}
  {% set has_loaded_at_col = loaded_at_col is not none %}

  {% set where_exprs = [] %}

  {% if is_incremental_run and has_loaded_at_col %}
      {% set where_exprs = where_exprs + ["(select max(_loaded_at) from " ~ this ~ ") < " ~ loaded_at_col] %}
  {% endif %}

  {% if exclude_data and has_loaded_at_col %}
      {% set where_exprs = where_exprs + [loaded_at_col ~ " <= '" ~ run_started_at.strftime('%Y-%m-%d %H:%M:%S') ~ "'"] %}
  {% endif %}

  {% if where_exprs | length == 0 %}
    {{ return(source_relation) }}
  {% else %}
    {% set where_clause = where_exprs | join(' and ') %}
    {{ return("(select * from " ~ source_relation ~ " where " ~ where_clause ~ ")") }}
  {% endif %}

{% endmacro %}
