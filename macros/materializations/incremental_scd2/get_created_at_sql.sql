{%- docs get_created_at_sql -%}
Generates SQL for the created_at timestamp in SCD Type 2 records.

Creates a window function that returns the earliest valid_from timestamp
for each unique key, representing when the first version of this record
was created in the dimension table.

**Args:**
- `unique_keys_csv` (string): Comma-separated list of business key columns
- `updated_at_col` (string): Column name containing the record's update timestamp

**Returns:**
- SQL expression using FIRST_VALUE window function to get the earliest timestamp per key

**Example:**
For a customer record that first appeared on 2021-06-01 and was updated on 2021-08-01,
both records will have created_at = 2021-06-01, indicating when this customer
was first introduced to the dimension.
{%- enddocs -%}

{% macro get_created_at_sql(unique_keys_csv, updated_at_col) -%}
  first_value({{ updated_at_col }}) over (partition by {{ unique_keys_csv }} order by {{ updated_at_col }})
{%- endmacro %}