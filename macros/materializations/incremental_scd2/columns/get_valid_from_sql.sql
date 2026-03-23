{#
  Generates the valid_from SQL expression for SCD Type 2 records.

  Args:
    updated_at_col (string): Column containing the record's update timestamp.
    created_at_col (string, optional): Column containing the record's creation timestamp.
      When provided, the first version of each record uses created_at as valid_from
      instead of updated_at.

  Returns:
    SQL expression for the valid_from timestamp.

  Example:
    For a customer record updated on 2021-06-01, the valid_from will be 2021-06-01.
#}

{% macro get_valid_from_sql(updated_at_col, created_at_col=None) -%}
  {#
    If created_at_col is provided, use it for the first record (no previous record for the entity),
    otherwise fall back to updated_at_col for all records.
  #}
  {%- if created_at_col is not none -%}
    case
      when lag({{ updated_at_col }}) over (partition by {{ created_at_col }} order by {{ updated_at_col }}) is null
        then {{ created_at_col }}::timestamp_tz
      else {{ updated_at_col }}::timestamp_tz
    end
  {%- else -%}
    {{ updated_at_col }}::timestamp_tz
  {%- endif -%}
{%- endmacro %}
