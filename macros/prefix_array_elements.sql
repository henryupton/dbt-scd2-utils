{#
  Prefixes each element of a list with a given string.

  Args:
    list_items (array): Array of strings to prefix
    prefix (string): String to prefix each element with

  Returns:
    Array: New array with each element prefixed

  Example:
    prefix_array_elements(["col1", "col2"], "a.") returns ["a.col1", "a.col2"]
    prefix_array_elements(["customer_id"], "t.") returns ["t.customer_id"]

  Raises:
    Compiler error if list_items is not an array or contains non-string elements
#}

{%- macro prefix_array_elements(list_items, prefix) -%}
  {%- if not dbt_scd2_utils.is_array(list_items) -%}
    {%- set error_message -%}
      prefix_array_elements expects an array of strings.
      Received: {{ list_items }} ({{ list_items.__class__.__name__ }})
    {%- endset -%}
    {{ exceptions.raise_compiler_error(error_message) }}
  {%- endif -%}
  
  {%- for item in list_items -%}
    {%- if item is not string -%}
      {%- set error_message -%}
        prefix_array_elements expects all array elements to be strings.
        Found non-string element: {{ item }} ({{ item.__class__.__name__ }})
      {%- endset -%}
      {{ exceptions.raise_compiler_error(error_message) }}
    {%- endif -%}
  {%- endfor -%}
  
  {%- set prefixed_items = [] -%}
  {%- for item in list_items -%}
    {%- set prefixed_items = prefixed_items.append(prefix ~ item) -%}
  {%- endfor -%}
  
  {{ return(prefixed_items) }}
{%- endmacro -%}