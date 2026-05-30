{#
  Generates SQL for the initial load of an SCD Type 1 table.

  SCD Type 1 keeps exactly one row per business key. The source is deduplicated
  to the latest row per key (by updated_at), and the standard audit columns are
  attached as constants so the table shares the same signature as SCD Type 2:

    _is_current   -> always true
    _valid_from   -> coalesce(created_at, updated_at)
    _valid_to     -> default_valid_to
    _change_type  -> 'I'

  Args:
    arg_dict (dict): Configuration dictionary containing:
      temp_relation: Temporary table with source data
      unique_key: Array of business key columns
      dest_columns: Source column metadata
      is_current_column / valid_from_column / valid_to_column /
      updated_at_column / created_at_column / change_type_column: audit names

  Returns:
    SELECT SQL statement that includes original columns plus SCD audit columns
#}

{% macro get_initial_load_scd1_sql(arg_dict) %}
    {% set temp_relation = arg_dict["temp_relation"] %}
    {% set unique_key = arg_dict["unique_key"] %}
    {% set dest_columns = arg_dict["dest_columns"] %}

    {%- set is_current_col = arg_dict['is_current_column'] -%}
    {%- set valid_from_col = arg_dict['valid_from_column'] -%}
    {%- set valid_to_col = arg_dict['valid_to_column'] -%}
    {%- set updated_at_col = arg_dict['updated_at_column'] -%}
    {%- set created_at_col = arg_dict.get('created_at_column') -%}
    {%- set change_type_col = arg_dict['change_type_column'] -%}

    {%- set unique_keys_csv = dbt_scd2_utils.get_quoted_csv(unique_key) -%}
    {%- set select_cols = dest_columns | map(attribute="name") | list -%}

with source_data as (
    select * from {{ temp_relation }}
),

{# One row per key: keep the most recent version by updated_at. #}
dedup as (
    select *
    from source_data
    qualify row_number() over (partition by {{ unique_keys_csv }} order by {{ updated_at_col }} desc) = 1
)

select
    {{ dbt_scd2_utils.get_quoted_csv(select_cols) }},

    {# SCD1 audit columns: a single, always-current version per key. #}
    true as {{ is_current_col }},
    {% if created_at_col is not none -%}
    coalesce({{ created_at_col }}, {{ updated_at_col }})::timestamp_tz
    {%- else -%}
    {{ updated_at_col }}::timestamp_tz
    {%- endif %} as {{ valid_from_col }},
    {{ dbt_scd2_utils.parse_timestamp_literal(var('default_valid_to', '2999-12-31 23:59:59')) }} as {{ valid_to_col }},
    'I' as {{ change_type_col }}
from dedup

{% endmacro %}
