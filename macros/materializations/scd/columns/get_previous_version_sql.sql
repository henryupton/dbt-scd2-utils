{#
  Builds the expression for the optional `_previous` audit column: an OBJECT holding the
  tracked change columns of the immediately preceding version of the entity.

  Uses lag() over the key's timeline, so the first version of a key yields NULL. Keys are
  lowercased; object_construct_keep_null keeps null-valued keys so a genuinely-null prior
  value is still represented.

  Args:
    scd_check_columns (list): Tracked change columns (the hashed set).
    unique_keys_csv (string): Comma-separated business key columns for partitioning.
    updated_at_col (string): Column used to order the timeline.

  Returns:
    A SQL expression (lag of an object_construct_keep_null); no trailing alias.
#}

{%- macro get_previous_version_sql(scd_check_columns, unique_keys_csv, updated_at_col) -%}
lag(object_construct_keep_null(
  {%- for col in scd_check_columns %}
  '{{ col | lower }}', {{ col }}{{ "," if not loop.last }}
  {%- endfor %}
)) over (partition by {{ unique_keys_csv }} order by {{ updated_at_col }})
{%- endmacro -%}
