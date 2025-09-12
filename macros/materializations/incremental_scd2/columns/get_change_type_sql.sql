{#
  Generates SQL to determine the change type for SCD Type 2 records.

  Uses a window function to identify whether each record represents an INSERT ('I') 
  or UPDATE ('U') operation based on its chronological order within each unique key group.

  Args:
    unique_keys_csv (string): Comma-separated list of unique key columns for partitioning
    updated_at_col (string): Column name used for ordering records chronologically

  Returns:
    SQL CASE expression that returns 'I' for the first record per unique key, 'U' for subsequent records

  Example:
    For a customer with 3 versions (2021-01-01, 2021-06-01, 2021-12-01):
    - First record (2021-01-01) gets change_type = 'I' (initial insert)
    - Second record (2021-06-01) gets change_type = 'U' (update)
    - Third record (2021-12-01) gets change_type = 'U' (update)
#}

{%- macro get_change_type_sql(unique_keys_csv, updated_at_col) -%}
case when row_number() over (partition by {{ unique_keys_csv }} order by {{ updated_at_col }}) = 1 then 'I' else 'U' end
{%- endmacro -%}
