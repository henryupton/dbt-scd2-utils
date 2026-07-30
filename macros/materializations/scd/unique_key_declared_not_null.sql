{#
  Returns true only when EVERY unique_key column carries a declared `not_null` constraint on
  the model node (from schema.yml `constraints:` / a contract). Snowflake enforces NOT NULL
  (the only enforced constraint), so a declared-and-enforced not-null key is a hard guarantee
  that plain `=` matching is safe, letting us skip the runtime null guard.

  Best effort: if columns aren't declared, or the constraint isn't present, returns false and
  the caller falls back to the runtime guard. Never raises.

  Args:
    unique_key (array): the business key columns.
#}
{%- macro unique_key_declared_not_null(unique_key) -%}
  {%- set cols = model.get('columns', {}) if model is not none else {} -%}
  {%- if not cols -%}
    {{ return(false) }}
  {%- endif -%}

  {# Case-insensitive lookup: unique_key casing may differ from the yml declaration. #}
  {%- set lc = {} -%}
  {%- for name, info in cols.items() -%}
    {%- do lc.update({name | lower: info}) -%}
  {%- endfor -%}

  {%- for key in unique_key -%}
    {%- set info = lc.get(key | lower) -%}
    {%- if info is none -%}
      {{ return(false) }}
    {%- endif -%}
    {%- set ns = namespace(found=false) -%}
    {%- for c in info.get('constraints', []) -%}
      {%- set ctype = c.get('type', none) if c is mapping else c.type -%}
      {%- if ctype == 'not_null' -%}
        {%- set ns.found = true -%}
      {%- endif -%}
    {%- endfor -%}
    {%- if not ns.found -%}
      {{ return(false) }}
    {%- endif -%}
  {%- endfor -%}

  {{ return(true) }}
{%- endmacro -%}
