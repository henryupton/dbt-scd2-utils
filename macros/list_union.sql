{%- docs list_union -%}
Returns the union of any number of lists with deduplication.

**Args:**
- `lists` (list): A list of lists to union, or individual lists as separate arguments
- `case_insensitive` (bool, optional): If true, performs case-insensitive comparison. Defaults to false.

**Returns:**
- Combined list with unique elements from all input lists

**Example:**
```sql
{{ dbt_scd2_utils.list_union(['a', 'b'], ['B', 'c']) }}
-- Returns: ['a', 'b', 'B', 'c']

{{ dbt_scd2_utils.list_union(['a', 'b'], ['B', 'c'], ['C', 'd'], case_insensitive=true) }}
-- Returns: ['a', 'b', 'c', 'd']

{{ dbt_scd2_utils.list_union([['a', 'b'], ['B', 'c'], ['C', 'd']], case_insensitive=true) }}
-- Returns: ['a', 'b', 'c', 'd']
```
{%- enddocs -%}

{% macro list_union() %}
  {%- set all_args = varargs | list -%}
  {%- set case_insensitive = kwargs.get('case_insensitive', false) -%}
  
  {%- if all_args | length == 0 -%}
    {{ return([]) }}
  {%- endif -%}
  
  {# Check if first argument is a list of lists #}
  {%- set lists_to_process = [] -%}
  {%- if all_args | length == 1 and all_args[0] is iterable and all_args[0][0] is iterable -%}
    {# Single argument is a list of lists #}
    {%- set lists_to_process = all_args[0] -%}
  {%- else -%}
    {# Multiple list arguments #}
    {%- set lists_to_process = all_args -%}
  {%- endif -%}
  
  {%- set result = [] -%}
  {%- set seen = [] -%}
  
  {# Process each list #}
  {%- for list_item in lists_to_process -%}
    {%- for item in list_item -%}
      {%- set compare_item = item | upper if case_insensitive else item -%}
      {%- set compare_seen = seen | map('upper') | list if case_insensitive else seen -%}
      {%- if compare_item not in compare_seen -%}
        {%- do result.append(item) -%}
        {%- do seen.append(item) -%}
      {%- endif -%}
    {%- endfor -%}
  {%- endfor -%}
  
  {{ return(result) }}
{% endmacro %}