{%- docs source -%}
Override of dbt's built-in source() macro to add incremental loading capability.

This enhanced source macro adds optional incremental loading by comparing a
loaded_at timestamp column against the maximum value in the target table.
This is useful for efficiently loading only new records from source systems.

**Args:**
- `source_name` (string): The name of the source as defined in sources.yml
- `table_name` (string): The name of the table within the source
- `loaded_at_col` (string, optional): Column name for incremental comparison

**Returns:**
- SQL that selects from the source table, optionally filtered for incremental loads

**Behavior:**
- If running incrementally AND loaded_at_col is provided: Filters for records
  where loaded_at_col > max(loaded_at_col) from target table
- Otherwise: Returns the full source table (standard dbt behavior)

**Example:**
```sql
select * from {{ source('raw_data', 'customers', 'updated_at') }}
```
On incremental runs, this will only select customers with updated_at timestamps
newer than the latest record already in the target table.
{%- enddocs -%}

{% macro source(source_name, table_name, loaded_at_col=none) %}
  {% set source_relation = builtins.source(source_name, table_name) %}

  {% if dbt_scd2_utils.is_incremental() and loaded_at_col is not none %}

  (
    select *
    from {{ source_relation }}
    where {{ loaded_at_col }} > (select max({{ loaded_at_col }}) from {{ this }})
  )

  {% else %}

    {{ source_relation }}

  {% endif %}

{% endmacro %}
