{#
  Builds the and-joined per-column key match predicate shared by the SCD2 incremental
  `previous_record` lookup and the MERGE `ON`. Both must use the SAME predicate, or a
  null-bearing key can be pulled into the lookup but fall through the merge (or vice versa),
  stranding versions — the null_key regression suite guards exactly this invariant.

  null_safe=false emits plain `=`, which Snowflake can prune on and Search Optimization can
  accelerate. null_safe=true emits `equal_null` (NULL = NULL is true), which is correct for a
  nullable key but is not Search-Optimization-eligible.

  Args:
    columns (array): key columns to match.
    left_alias (string): table alias on the left side, without the dot, e.g. 'p' or
      'DBT_INTERNAL_DEST'. The macro adds the '.'.
    right_alias (string): table alias on the right side, without the dot, e.g. 'n' or
      'DBT_INTERNAL_SOURCE'.
    null_safe (bool): true -> equal_null, false -> plain =.
#}
{%- macro get_key_match_sql(columns, left_alias, right_alias, null_safe) -%}
{%- for col in columns -%}
{%- if null_safe -%}
equal_null({{ left_alias }}.{{ col }}, {{ right_alias }}.{{ col }})
{%- else -%}
{{ left_alias }}.{{ col }} = {{ right_alias }}.{{ col }}
{%- endif -%}
{{ " and " if not loop.last }}
{%- endfor -%}
{%- endmacro -%}
