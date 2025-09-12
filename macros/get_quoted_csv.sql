{#
  Converts column names (string or array) into a comma-separated list for SQL queries.

  Args:
    column_names (string|array): Either a single column name string or an array of column names
    table_alias (string, optional): Optional table alias to prefix each column name

  Returns:
    String: Comma-separated list of column names suitable for SQL queries

  Example:
    get_quoted_csv("col1") returns "col1"
    get_quoted_csv(["col1", "col2"]) returns "col1, col2"
    get_quoted_csv(["col1", "col2"], "a.") returns "a.col1, a.col2"
#}

{%- macro get_quoted_csv(column_names, table_alias='') -%}
  {%- if column_names is string -%}
    {{ table_alias }}{{ column_names }}
  {%- elif table_alias -%}
    {%- set prefixed_columns = dbt_scd2_utils.prefix_array_elements(column_names, table_alias) -%}
    {%- for column in prefixed_columns -%}
      {{ column }}{{ ", " if not loop.last }}
    {%- endfor -%}
  {%- else -%}
    {%- for column in column_names -%}
      {{ column }}{{ ", " if not loop.last }}
    {%- endfor -%}
  {%- endif -%}
{%- endmacro -%}
