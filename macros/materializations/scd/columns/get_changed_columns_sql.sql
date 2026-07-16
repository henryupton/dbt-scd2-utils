{#
  Builds the expression for the optional `_changed` audit column: an OBJECT with one
  lowercased key per tracked change column, true when that column differs from the prior
  version, false otherwise.

  Comparison is on the VARCHAR cast of each value, deliberately matching the representation
  dbt_utils.generate_surrogate_key uses for change detection. This keeps `_changed`
  consistent with version creation: a new version exists only because the hash of the
  varchar casts differed, so at least one tracked column will read as changed. Comparing the
  raw typed values with IS DISTINCT FROM instead can disagree with versioning (e.g. two
  TIMESTAMP_TZ values at the same instant but different UTC offsets are equal as timestamps
  yet cast to different strings, so they create a version but would show no change).
  IS DISTINCT FROM over the casts still treats NULLs correctly (null vs null is not a change,
  null vs value is).

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
  when row_number() over (partition by {{ unique_keys_csv }} order by {{ updated_at_col }}) = 1
    then cast(null as object)
  else object_construct_keep_null(
    {%- for col in scd_check_columns %}
    '{{ col | lower }}', (cast({{ col }} as varchar) is distinct from lag(cast({{ col }} as varchar)) over (partition by {{ unique_keys_csv }} order by {{ updated_at_col }})){{ "," if not loop.last }}
    {%- endfor %}
  )
end
{%- endmacro -%}
