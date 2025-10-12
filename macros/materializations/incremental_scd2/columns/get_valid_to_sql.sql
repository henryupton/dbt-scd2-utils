{#
  Generates SQL to determine the valid_to timestamp for SCD Type 2 records.

  Uses the LEAD window function to get the next record's updated_at timestamp as this
  record's valid_to date. For the most current record, uses the default_valid_to value.

  When a deleted_at column is provided:
  - For non-deletion records (deleted_at IS NULL): valid_to follows normal logic
  - For deletion records (deleted_at IS NOT NULL): valid_to is the next record's updated_at
    or the default future date, allowing the deletion record to span until resurrection or forever

  Args:
    unique_keys_csv (string): Comma-separated list of unique key columns for partitioning
    updated_at_col (string): Column name used for ordering records chronologically
    default_valid_to (string): Default timestamp for current records (e.g., '2999-12-31')
    deleted_at_col (string, optional): Column name containing logical deletion timestamp

  Returns:
    SQL expression that returns the valid_to timestamp for each record

  Example:
    For a product with records (2021-01-01, 2021-06-01 [deleted], 2021-12-01):
    - First record: valid_to = 2021-06-01 (next record's date)
    - Second record (deleted): valid_to = 2021-12-01 (next record's date, not deleted_at)
    - Third record (resurrected): valid_to = 2999-12-31 (default, as it's current)
#}

{% macro get_valid_to_sql(unique_keys_csv, updated_at_col, default_valid_to, deleted_at_col) -%}
  coalesce(
    lead({{ updated_at_col }}) over(partition by {{ unique_keys_csv }} order by {{ updated_at_col }}),
    {{ dbt_scd2_utils.parse_timestamp_literal(var('default_valid_to', '2999-12-31 23:59:59')) }}
  )
{%- endmacro %}