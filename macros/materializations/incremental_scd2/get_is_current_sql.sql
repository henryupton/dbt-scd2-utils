{%- docs get_is_current_sql -%}
Generates SQL to determine if a record is the current/active version for SCD Type 2.

Returns a boolean expression that evaluates to true for the most recent record
per unique key combination (based on updated_at timestamp).

**Args:**
- `unique_keys_csv` (string): Comma-separated list of unique key columns for partitioning
- `updated_at_col` (string): Column name used for ordering records by recency

**Returns:**
- SQL expression that evaluates to true for current records, false for historical ones

**Example:**
For a customer with multiple versions, only the record with the latest updated_at
timestamp will have is_current = true.
{%- enddocs -%}

{%- macro get_is_current_sql(unique_keys_csv, updated_at_col) -%}
  row_number() over(partition by {{ unique_keys_csv }} order by {{ updated_at_col }} desc) = 1
{%- endmacro -%}