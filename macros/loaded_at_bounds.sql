{#
  Predicates bounding a load timestamp to the window the `loaded_from` / `loaded_to` vars set.

  Both vars are optional timestamps and the window is half-open: column >= loaded_from and
  column < loaded_to. Give each an explicit offset: a bare value is read in the session timezone.
  With neither set the list is empty. The legacy `exclude_data_after_run_start` var stands in for
  `loaded_to` as the run start time when `loaded_to` is unset.

  Both the source() override and any project ref() override call this, so every read in a run is
  bounded the same way: set `loaded_to` and the run sees the warehouse as it stood at that moment.

  Args:
    column (string): Column or expression holding the load timestamp

  Returns:
    list of SQL predicate strings, empty when no bound is set

  Example:
    dbt build --vars "{loaded_to: '2026-11-25 00:00:00 +00:00'}"
    dbt_scd2_utils.loaded_at_bounds('_loaded_at') returns ["_loaded_at < '2026-11-25 00:00:00 +00:00'::timestamp_tz"]
#}

{%- macro loaded_at_bounds(column) -%}
  {%- set loaded_from = var('loaded_from', none) -%}
  {%- set loaded_to = var('loaded_to', none) -%}
  {%- if loaded_to is none and var('exclude_data_after_run_start', false) -%}
    {%- set loaded_to = run_started_at.strftime('%Y-%m-%d %H:%M:%S') ~ ' +00:00' -%}
  {%- endif -%}

  {%- set predicates = [] -%}
  {%- if loaded_from is not none -%}
    {%- do predicates.append(column ~ " >= " ~ dbt_scd2_utils.parse_timestamp_literal(loaded_from)) -%}
  {%- endif -%}
  {%- if loaded_to is not none -%}
    {%- do predicates.append(column ~ " < " ~ dbt_scd2_utils.parse_timestamp_literal(loaded_to)) -%}
  {%- endif -%}
  {{ return(predicates) }}
{%- endmacro -%}
