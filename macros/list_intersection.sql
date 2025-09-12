{#
  Returns the intersection of any number of lists with deduplication.

  Args:
    lists (list): A list of lists to intersect, or individual lists as separate arguments
    case_insensitive (bool, optional): If true, performs case-insensitive comparison. Defaults to false.

  Returns:
    List containing only elements that appear in all input lists

  Example:
    {{ dbt_scd2_utils.list_intersection(['a', 'b', 'c'], ['B', 'c', 'd']) }}
    -- Returns: ['c']

    {{ dbt_scd2_utils.list_intersection(['a', 'b', 'c'], ['B', 'c', 'd'], ['c', 'e'], case_insensitive=true) }}
    -- Returns: ['c']

    {{ dbt_scd2_utils.list_intersection([['a', 'b', 'c'], ['B', 'c', 'd'], ['c', 'e']], case_insensitive=true) }}
    -- Returns: ['c']
#}

{% macro list_intersection() %}
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
  
  {%- if lists_to_process | length == 1 -%}
    {{ return(lists_to_process[0] | list) }}
  {%- endif -%}
  
  {%- set result = [] -%}
  {%- set seen = [] -%}
  
  {# Start with the first list as base #}
  {%- set base_list = lists_to_process[0] -%}
  
  {# Create sets of all other lists for comparison #}
  {%- set comparison_sets = [] -%}
  {%- for i in range(1, lists_to_process | length) -%}
    {%- set comparison_set = lists_to_process[i] | map('upper') | list if case_insensitive else (lists_to_process[i] | list) -%}
    {%- do comparison_sets.append(comparison_set) -%}
  {%- endfor -%}
  
  {%- for item in base_list -%}
    {%- set compare_item = item | upper if case_insensitive else item -%}
    {%- set compare_seen = seen | map('upper') | list if case_insensitive else seen -%}
    
    {# Check if item is in all other lists #}
    {%- set in_all_lists = true -%}
    {%- for comparison_set in comparison_sets -%}
      {%- if compare_item not in comparison_set -%}
        {%- set in_all_lists = false -%}
      {%- endif -%}
    {%- endfor -%}
    
    {%- if in_all_lists and compare_item not in compare_seen -%}
      {%- do result.append(item) -%}
      {%- do seen.append(item) -%}
    {%- endif -%}
  {%- endfor -%}
  
  {{ return(result) }}
{% endmacro %}