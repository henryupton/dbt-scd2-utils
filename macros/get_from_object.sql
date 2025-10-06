{#
  Gets a value from a nested object/array using a path of keys and/or indices. Returns default value if the path doesn't exist.

  Args:
    obj (dict|list): The object or array to traverse
    *path_parts (varargs): Variable number of keys (for dicts) or indices (for lists) to traverse
    default (any, optional): Value to return if path not found. Defaults to none.

  Returns:
    The value at the specified path, or default if not found

  Examples:
    {% set my_obj = {'a': {'b': {'c': 'value'}}} %}
    {{ dbt_scd2_utils.get_from_object(my_obj, 'a', 'b', 'c') }}
    -- Returns: 'value'

    {% set mixed = {'items': [{'name': 'first'}, {'name': 'second'}]} %}
    {{ dbt_scd2_utils.get_from_object(mixed, 'items', 0, 'name') }}
    -- Returns: 'first'

    {{ dbt_scd2_utils.get_from_object(mixed, 'items', 1, 'name') }}
    -- Returns: 'second'

    {{ dbt_scd2_utils.get_from_object(my_obj, 'a', 'x', 'y') }}
    -- Returns: null

    {{ dbt_scd2_utils.get_from_object(my_obj, 'a', 'x', 'y', default='not found') }}
    -- Returns: 'not found'
#}

{% macro get_from_object(obj) %}
  {%- set default = kwargs.get('default', none) -%}
  {%- set path_parts = varargs -%}

  {# Base case: no more path parts, return current object #}
  {%- if (path_parts | length) == 0 -%}
    {{ return(obj) }}
  {%- endif -%}

  {# Get first path part and remaining parts #}
  {%- set key = path_parts[0] -%}
  {%- set remaining = path_parts[1:] -%}

  {# If obj is a mapping and key exists, recurse #}
  {%- if obj is mapping and key in obj -%}
    {{ return(dbt_scd2_utils.get_from_object(obj[key], *remaining, default=default)) }}
  {%- endif -%}

  {# If obj is an array and key is a valid index, recurse #}
  {%- if dbt_scd2_utils.is_array(obj) -%}
    {%- if key is number and key >= 0 and key < (obj | length) -%}
      {{ return(dbt_scd2_utils.get_from_object(obj[key], *remaining, default=default)) }}
    {%- endif -%}
  {%- endif -%}

  {# If we get here, path doesn't exist - return default #}
  {{ return(default) }}
{% endmacro %}
