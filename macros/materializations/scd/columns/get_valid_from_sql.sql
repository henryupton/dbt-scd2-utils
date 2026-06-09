{#
  Generates SQL to determine the valid_from timestamp for SCD Type 2 records.

  For the FIRST version of a key (no previous record) valid_from is
  coalesce(deleted_at, created_at, updated_at):
    - a born-deleted record (deleted_at set) is valid from the deletion time;
    - otherwise created_at when configured;
    - otherwise updated_at.
  Non-first versions always use updated_at, so each window starts where the
  previous version's valid_to (lead(updated_at)) ends.

  Args:
    unique_keys_csv (string): Comma-separated unique key columns for partitioning.
    updated_at_col (string): Column used for chronological ordering.
    created_at_col (string, optional): Creation timestamp; first version of a key
      uses it when there is no deleted_at.
    deleted_at_col (string, optional): Logical deletion timestamp; a born-deleted
      first version uses it.

  Returns:
    SQL expression returning the valid_from timestamp for each record.
#}

{%- macro get_valid_from_sql(unique_keys_csv, updated_at_col, created_at_col=None, deleted_at_col=None) -%}
  {%- set first_record_parts = [] -%}
  {%- if deleted_at_col is not none -%}
    {%- do first_record_parts.append(deleted_at_col ~ '::timestamp_tz') -%}
  {%- endif -%}
  {%- if created_at_col is not none -%}
    {%- do first_record_parts.append(created_at_col ~ '::timestamp_tz') -%}
  {%- endif -%}
  {%- do first_record_parts.append(updated_at_col ~ '::timestamp_tz') -%}

  {%- if deleted_at_col is not none or created_at_col is not none -%}
    case
      when lag({{ updated_at_col }}) over (partition by {{ unique_keys_csv }} order by {{ updated_at_col }}) is null
        then coalesce({{ first_record_parts | join(', ') }})
      else {{ updated_at_col }}::timestamp_tz
    end
  {%- else -%}
    {{ updated_at_col }}::timestamp_tz
  {%- endif -%}
{%- endmacro %}
