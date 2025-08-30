{%- docs is_array -%}
Checks if a given object is an array (list).

**Args:**
- `obj` (any): The object to check

**Returns:**
- Boolean: True if the object is an array, False otherwise

**Example:**
- `is_array("string")` returns `False`
- `is_array(["item1", "item2"])` returns `True`
- `is_array([])` returns `True`
{%- enddocs -%}

{%- macro is_array(obj) -%}
  {{ obj is iterable and obj is not string and obj is not mapping }}
{%- endmacro -%}