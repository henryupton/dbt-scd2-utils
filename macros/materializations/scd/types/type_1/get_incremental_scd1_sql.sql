{#
  Generates the SQL for incremental SCD Type 1 processing using a MERGE statement.

  SCD Type 1 keeps a single row per business key, overwritten in place. There is
  no history and no change detection: the source is deduplicated to one row per
  key and merged on the business key alone.

    - matched     -> overwrite the business columns with the latest values.
                     Audit columns are deliberately left untouched, so
                     _valid_from (first-seen) is preserved and _is_current,
                     _valid_to and _change_type keep their constant values.
    - not matched -> insert a new row with the SCD1 audit columns.

  Args:
    arg_dict (dict): Configuration dictionary containing:
      target_relation: The target table to merge into
      temp_relation: Temporary table with new data
      unique_key: Array of business key columns
      dest_columns: Source column metadata
      audit_columns: List of audit column names
      is_current_column / valid_from_column / valid_to_column /
      updated_at_column / created_at_column / change_type_column: audit names

  Returns:
    Complete MERGE SQL statement for SCD Type 1 incremental processing
#}

{% macro get_incremental_scd1_sql(arg_dict) %}
    {% set target_relation = arg_dict["target_relation"] %}
    {% set temp_relation = arg_dict["temp_relation"] %}
    {% set unique_key = arg_dict["unique_key"] %}
    {% set dest_columns = arg_dict["dest_columns"] %}
    {%- set audit_cols_names = arg_dict["audit_columns"] -%}

    {%- set is_current_col = arg_dict['is_current_column'] -%}
    {%- set valid_from_col = arg_dict['valid_from_column'] -%}
    {%- set valid_to_col = arg_dict['valid_to_column'] -%}
    {%- set updated_at_col = arg_dict['updated_at_column'] -%}
    {%- set created_at_col = arg_dict.get('created_at_column') -%}
    {%- set change_type_col = arg_dict['change_type_column'] -%}

    {%- set unique_keys_csv = dbt_scd2_utils.get_quoted_csv(unique_key | map("upper")) -%}
    {%- set all_dest_columns = dest_columns | map(attribute='name') | map('upper') | list -%}

    {# Business columns are everything in the source minus the audit columns. #}
    {%- set business_cols = dbt_scd2_utils.list_difference(all_dest_columns, audit_cols_names, case_insensitive=true) -%}
    {%- set business_cols_csv = dbt_scd2_utils.get_quoted_csv(business_cols) -%}

    {# On a match we overwrite the business columns, but never the key columns. #}
    {%- set update_cols = dbt_scd2_utils.list_difference(business_cols, unique_key, case_insensitive=true) -%}

    {%- set all_cols_names = business_cols + audit_cols_names -%}
    {%- set all_cols_csv = dbt_scd2_utils.get_quoted_csv(all_cols_names) -%}

merge into {{ target_relation }} AS DBT_INTERNAL_DEST
using (
    with source_data as (
        select {{ business_cols_csv }}
        from {{ temp_relation }}
    ),

    {# One row per key: keep the most recent version by updated_at. #}
    dedup as (
        select *
        from source_data
        qualify row_number() over (partition by {{ unique_keys_csv }} order by {{ updated_at_col }} desc) = 1
    )

    select
        {{ business_cols_csv }},
        true as {{ is_current_col }},
        {% if created_at_col is not none -%}
        coalesce({{ created_at_col }}, {{ updated_at_col }})::timestamp_tz
        {%- else -%}
        {{ updated_at_col }}::timestamp_tz
        {%- endif %} as {{ valid_from_col }},
        {{ dbt_scd2_utils.parse_timestamp_literal(var('default_valid_to', '2999-12-31 23:59:59')) }} as {{ valid_to_col }},
        'I' as {{ change_type_col }}
    from dedup
) AS DBT_INTERNAL_SOURCE
on (
    {% for col in unique_key -%}
        DBT_INTERNAL_DEST.{{ col }} = DBT_INTERNAL_SOURCE.{{ col }}{% if not loop.last %} and {% endif %}
    {%- endfor %}
)
{# Overwrite the latest business values; leave the audit columns as they are. #}
when matched then update set
    {% for col in update_cols %}
        DBT_INTERNAL_DEST.{{ col }} = DBT_INTERNAL_SOURCE.{{ col }}{% if not loop.last %},{% endif %}
    {%- endfor %}
{# New key: insert the row with its SCD1 audit columns. #}
when not matched then insert ({{ all_cols_csv }})
values ({{ all_cols_csv }})
{% endmacro %}
