{%- docs get_valid_from_sql -%}
Generates SQL for the valid_from timestamp in SCD Type 2 records.

Simple macro that returns the updated_at column as the valid_from date,
indicating when this version of the record became active.

**Args:**
- `updated_at_col` (string): Column name containing the record's update timestamp

**Returns:**
- SQL expression that returns the updated_at column value as valid_from

**Example:**
For a customer record updated on 2021-06-01, the valid_from will be 2021-06-01.
{%- enddocs -%}

{% macro get_valid_from_sql(updated_at_col) -%}
  {{ updated_at_col }}
{%- endmacro %}
