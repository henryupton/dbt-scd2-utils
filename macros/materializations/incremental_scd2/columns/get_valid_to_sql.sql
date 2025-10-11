{#
  Generates SQL to determine the valid_to timestamp for SCD Type 2 records.

  Uses the LEAD window function to get the next record's updated_at timestamp as this
  record's valid_to date. For the most current record, uses the default_valid_to value.

  If a deleted_at column is provided and contains a value, that timestamp will be used
  as the valid_to, effectively closing the record at the deletion time.

  Args:
    unique_keys_csv (string): Comma-separated list of unique key columns for partitioning
    updated_at_col (string): Column name used for ordering records chronologically
    default_valid_to (string): Default timestamp for current records (e.g., '2999-12-31')
    deleted_at_col (string, optional): Column name containing logical deletion timestamp

  Returns:
    SQL expression that returns the valid_to timestamp for each record

  Example:
    For a customer with 3 versions (2021-01-01, 2021-06-01, 2021-12-01):
    - First record valid_to = 2021-06-01 (next record's date)
    - Second record valid_to = 2021-12-01 (next record's date)
    - Third record valid_to = 2999-12-31 (default, as it's current)

    If deleted_at is provided and the third record has deleted_at = 2021-12-15:
    - Third record valid_to = 2021-12-15 (deletion timestamp)
#}

{% macro get_valid_to_sql(unique_keys_csv, updated_at_col, default_valid_to, deleted_at_col) -%}
  {%- if deleted_at_col -%}
  coalesce(
    {{ deleted_at_col }},
    lead({{ updated_at_col }}) over(partition by {{ unique_keys_csv }} order by {{ updated_at_col }}),
    {{ dbt_scd2_utils.parse_timestamp_literal(var('default_valid_to', '2999-12-31 23:59:59')) }}
  )
  {%- else -%}
  coalesce(
    lead({{ updated_at_col }}) over(partition by {{ unique_keys_csv }} order by {{ updated_at_col }}),
    {{ dbt_scd2_utils.parse_timestamp_literal(var('default_valid_to', '2999-12-31 23:59:59')) }}
  )
  {%- endif -%}
{%- endmacro %}