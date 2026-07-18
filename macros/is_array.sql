{#
  Checks if a given object is an array (list).

  Args:
    obj (any): The object to check

  Returns:
    Boolean: True if the object is an array, False otherwise

  Example:
    is_array("string") returns False
    is_array(["item1", "item2"]) returns True
    is_array([]) returns True
#}

{%- macro is_array(obj) -%}
  {# return() yields a real boolean; a bare {{ ... }} would render the string "True"/"False", #}
  {# which is always truthy in an {% if %} and silently breaks every caller's condition. #}
  {{ return(obj is iterable and obj is not string and obj is not mapping) }}
{%- endmacro -%}