{#
  Generates SQL to determine the change type for SCD Type 2 records.

  Uses window functions to identify whether each record represents an INSERT ('I'),
  UPDATE ('U'), or DELETE ('D') operation based on chronological order and deleted_at status.

  Args:
    unique_keys_csv (string): Comma-separated list of unique key columns for partitioning
    updated_at_col (string): Column name used for ordering records chronologically
    deleted_at_col (string, optional): Column name containing deletion timestamp

  Returns:
    SQL CASE expression that returns:
    - 'I' for the first record per unique key or after a deletion (resurrection)
    - 'U' for regular updates
    - 'D' for deletion records (when deleted_at is not null)

  Example:
    For a customer with versions including deletions and resurrections:
    - First record (2021-01-01) gets change_type = 'I' (initial insert)
    - Second record (2021-06-01) gets change_type = 'U' (update)
    - Deletion record (2021-08-01, deleted_at is not null) gets change_type = 'D'
    - Resurrection record (2021-10-01, deleted_at is null) gets change_type = 'I' (re-insert)
#}

{%- macro get_change_type_sql(unique_keys_csv, updated_at_col, deleted_at_col) -%}
{%- if deleted_at_col -%}
case
  when {{ deleted_at_col }} is not null then 'D'
  when row_number() over (partition by {{ unique_keys_csv }} order by {{ updated_at_col }}) = 1 then 'I'
  when lag({{ deleted_at_col }}) over (partition by {{ unique_keys_csv }} order by {{ updated_at_col }}) is not null then 'I'
  else 'U'
end
{%- else -%}
case when row_number() over (partition by {{ unique_keys_csv }} order by {{ updated_at_col }}) = 1 then 'I' else 'U' end
{%- endif -%}
{%- endmacro -%}
