{#
  Describes how an existing SCD table's schema differs from what a full refresh would now
  produce, so the full-refresh path can decide whether it is safe to truncate + insert into
  the existing table or must fall back to create or replace.

  Business columns are compared by name and data type between the freshly built temp
  relation and the existing table (minus its audit columns), using dbt's own diff_columns /
  diff_column_data_types. A narrower incoming string column still fits the existing one and
  is not a change; a wider one is. Audit columns are checked by name only: the package fixes
  their types, so they only change on a package upgrade.

  Args:
    existing_relation: the existing target table.
    dest_columns: columns of the temp relation (business columns only).
    audit_columns (array): audit column names the initial load appends.

  Returns:
    none when the schemas match, otherwise a short human-readable summary of the differences
    for the log, e.g. "added: EMAIL; retyped: AMOUNT -> NUMBER(38,2)".
#}
{% macro get_scd_schema_changes(existing_relation, dest_columns, audit_columns) %}
  {%- set audit_upper = audit_columns | map('upper') | list -%}
  {%- set existing_columns = adapter.get_columns_in_relation(existing_relation) -%}
  {%- set existing_upper = existing_columns | map(attribute='name') | map('upper') | list -%}

  {%- set existing_business_columns = [] -%}
  {%- for column in existing_columns -%}
    {%- if (column.name | upper) not in audit_upper -%}
      {%- do existing_business_columns.append(column) -%}
    {%- endif -%}
  {%- endfor -%}

  {%- set added = diff_columns(dest_columns, existing_business_columns) | map(attribute='name') | list -%}
  {%- set removed = diff_columns(existing_business_columns, dest_columns) | map(attribute='name') | list -%}
  {%- set retyped = [] -%}
  {%- for change in diff_column_data_types(dest_columns, existing_business_columns) -%}
    {%- do retyped.append(change['column_name'] ~ ' -> ' ~ change['new_type']) -%}
  {%- endfor -%}
  {%- set missing_audit = dbt_scd2_utils.list_difference(audit_upper, existing_upper, case_insensitive=true) -%}

  {%- set changes = [] -%}
  {%- if added | length > 0 -%}{%- do changes.append('added: ' ~ (added | join(', '))) -%}{%- endif -%}
  {%- if removed | length > 0 -%}{%- do changes.append('removed: ' ~ (removed | join(', '))) -%}{%- endif -%}
  {%- if retyped | length > 0 -%}{%- do changes.append('retyped: ' ~ (retyped | join(', '))) -%}{%- endif -%}
  {%- if missing_audit | length > 0 -%}{%- do changes.append('missing audit columns: ' ~ (missing_audit | join(', '))) -%}{%- endif -%}

  {%- if changes | length == 0 -%}
    {{ return(none) }}
  {%- endif -%}
  {{ return(changes | join('; ')) }}
{% endmacro %}
