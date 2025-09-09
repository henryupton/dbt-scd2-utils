{%- docs list_difference -%}
Returns the difference of two lists (elements in first list but not in second) with deduplication.

**Args:**
- `list_a` (list): First list
- `list_b` (list): Second list
- `case_insensitive` (bool, optional): If true, performs case-insensitive comparison. Defaults to false.

**Returns:**
- List containing elements from list_a that are not in list_b

**Example:**
```sql
{{ dbt_scd2_utils.list_difference(['a', 'b', 'c'], ['B', 'd']) }}
-- Returns: ['a', 'b', 'c']

{{ dbt_scd2_utils.list_difference(['a', 'b', 'c'], ['B', 'd'], case_insensitive=true) }}
-- Returns: ['a', 'c']
```
{%- enddocs -%}

{% macro list_difference(list_a, list_b, case_insensitive=false) %}
  {%- set result = [] -%}
  {%- set seen = [] -%}
  {%- set list_b_set = list_b | map('upper') | list if case_insensitive else (list_b | list) -%}
  
  {%- for item in list_a -%}
    {%- set compare_item = item | upper if case_insensitive else item -%}
    {%- set compare_seen = seen | map('upper') | list if case_insensitive else seen -%}
    {%- if compare_item not in list_b_set and compare_item not in compare_seen -%}
      {%- do result.append(item) -%}
      {%- do seen.append(item) -%}
    {%- endif -%}
  {%- endfor -%}
  
  {{ return(result) }}
{% endmacro %}