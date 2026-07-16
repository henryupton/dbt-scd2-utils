{#
  Builds the expression for the optional `_changed` audit column: an OBJECT with one
  lowercased key per tracked change column, true when that column differs from the prior
  version (via IS DISTINCT FROM), false otherwise.

  The whole object is NULL for a key's first version (no prior to compare), matching the
  `_previous` column.

  Args:
    scd_check_columns (list): Tracked change columns (the hashed set).
    unique_keys_csv (string): Comma-separated business key columns for partitioning.
    updated_at_col (string): Column used to order the timeline.

  Returns:
    A SQL CASE expression; no trailing alias.
#}

{%- macro get_changed_columns_sql(scd_check_columns, unique_keys_csv, updated_at_col) -%}
case
  when lag({{ updated_at_col }}) over (partition by {{ unique_keys_csv }} order by {{ updated_at_col }}) is null
    then cast(null as object)
  else object_construct_keep_null(
    {%- for col in scd_check_columns %}
    '{{ col | lower }}', ({{ col }} is distinct from lag({{ col }}) over (partition by {{ unique_keys_csv }} order by {{ updated_at_col }})){{ "," if not loop.last }}
    {%- endfor %}
  )
end
{%- endmacro -%}
