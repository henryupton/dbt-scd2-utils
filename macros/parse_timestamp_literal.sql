{%- docs parse_timestamp_literal -%}
Parses a timestamp string literal to a timestamp with timezone using Snowflake syntax.

Converts a string timestamp literal into a properly typed timestamp_tz value
using Snowflake's :: casting syntax.

**Args:**
- `timestamp_string` (string): String representation of a timestamp

**Returns:**
- SQL expression that casts the timestamp string to timestamp_tz type

**Example:**
`parse_timestamp_literal('2999-12-31 23:59:59')` returns `'2999-12-31 23:59:59'::timestamp_tz`
{%- enddocs -%}

{% macro parse_timestamp_literal(timestamp_string) %}
  '{{ timestamp_string }}'::timestamp_tz
{% endmacro %}
