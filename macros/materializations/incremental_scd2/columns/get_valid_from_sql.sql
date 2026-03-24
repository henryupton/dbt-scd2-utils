{#
  Generates SQL to determine the valid_from timestamp for SCD Type 2 records.

  Args:
    unique_keys_csv (string): Comma-separated list of unique key columns for partitioning
    updated_at_col (string): Column name used for ordering records chronologically
    created_at_col (string, optional): Column containing the record's creation timestamp. When provided, the first version of each record uses created_at as valid_from instead of updated_at.

  Returns:
    SQL expression that returns the valid_from timestamp for each record

  Example:
    For a customer record updated on 2021-06-01, the valid_from will be 2021-06-01 unless it's the first record, in which case it uses created_at.
#}

{%- macro get_valid_from_sql(unique_keys_csv, updated_at_col, created_at_col=None) -%}
  {#
    If created_at_col is provided, use it for the first record (no previous record for the entity),
    otherwise fall back to updated_at_col for all records.
  #}
  {%- if created_at_col is not none -%}
    case
      when lag({{ updated_at_col }}) over (partition by {{ unique_keys_csv }} order by {{ updated_at_col }}) is null
        then {{ created_at_col }}::timestamp_tz
      else {{ updated_at_col }}::timestamp_tz
    end
  {%- else -%}
    {{ updated_at_col }}::timestamp_tz
  {%- endif -%}
{%- endmacro %}
