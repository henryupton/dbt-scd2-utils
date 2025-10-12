{#
  Generates SQL for the initial load of an SCD Type 2 table.

  Creates a SELECT statement that adds the necessary SCD audit columns to the source data
  for the first-time population of an SCD Type 2 table. This handles the bootstrap case
  where no historical data exists yet.

  Args:
    arg_dict (dict): Configuration dictionary containing:
      temp_relation: Temporary table with source data
      unique_key: Array of business key columns
      scd_check_columns: Optional columns to include in change detection
      audit_columns: List of audit column names to add
      is_current_column: Name of the is_current flag column
      valid_from_column: Name of the valid_from timestamp column
      valid_to_column: Name of the valid_to timestamp column
      updated_at_column: Name of the updated_at timestamp column
      change_type_column: Name of the change_type column

  Returns:
    SELECT SQL statement that includes original columns plus SCD audit columns

  Example:
    For an initial load with customer data, this will:
    - Set is_current = true for the latest version of each customer
    - Set valid_from = updated_at timestamp
    - Set valid_to = default_valid_to for current records, next updated_at for historical ones
    - Set change_type = 'I' for first record, 'U' for subsequent records
#}

{% macro get_initial_load_scd2_sql(arg_dict) %}
    {% set temp_relation = arg_dict["temp_relation"] %}
    {% set unique_key = arg_dict["unique_key"] %}
    {% set scd2_unique_key = arg_dict["scd2_unique_key"] %}
    {% set dest_columns = arg_dict["dest_columns"] %}
    {% set scd_check_columns = arg_dict["scd_check_columns"] %}
    {% set audit_columns = arg_dict["audit_columns"] %}
    
    {# Define our audit columns #}
    {%- set is_current_col = arg_dict.get('is_current_column') -%}
    {%- set valid_from_col = arg_dict.get('valid_from_column') -%}
    {%- set valid_to_col = arg_dict.get('valid_to_column') -%}
    {%- set updated_at_col = arg_dict.get('updated_at_column') -%}
    {%- set change_type_col = arg_dict.get('change_type_column') -%}
    {%- set deleted_at_col = arg_dict.get('deleted_at_column') -%}

    {# Prepare unique key CSV for window functions #}
    {%- set unique_keys_csv = dbt_scd2_utils.get_quoted_csv(unique_key) -%}
    {%- set select_cols = dest_columns | map(attribute="name") | list %}

with source_data as (
  select
    *,
    {{ dbt_utils.generate_surrogate_key(scd2_unique_key) }} as _scd2_key,
    {{ dbt_utils.generate_surrogate_key(scd_check_columns | list) }} as _scd2_hash,
  from {{ temp_relation }}
),

{# Make sure we have only one record for each unique key, updated_at permutation. #}
{# Prioritise existing record over a new one in the case of a duplicate. Why would something have changed but not produced a new updated_at? #}
pick_a_key_any_key as (
    select
        *
    from source_data
    qualify row_number() over(partition by _scd2_key order by 1) = 1
)
-- select * from pick_a_key_any_key order by {{ unique_keys_csv }}, {{ updated_at_col }} limit 123;
,

compare_versions as (
    select
        *,
        lag(_scd2_hash) over(partition by {{ unique_keys_csv }} order by {{ updated_at_col }}) as _prev_hash
    from pick_a_key_any_key
)
-- select * from compare_versions order by {{ unique_keys_csv }}, {{ updated_at_col }} limit 123;
,

{# This allows us to ignore changes in a certain subset of columns. #}
changes_only as (
    select *
    from compare_versions
    where (_prev_hash is null or _scd2_hash != _prev_hash) -- Only if the hash has changed (or is the first record for this key)
)
-- select * from changes_only order by {{ unique_keys_csv }}, {{ updated_at_col }} limit 123;

select
  {{ dbt_scd2_utils.get_quoted_csv(select_cols) }},

  {# Add SCD2 audit columns using reusable macros #}
  {{ dbt_scd2_utils.get_is_current_sql(unique_keys_csv, updated_at_col) }} as {{ is_current_col }},
  {{ dbt_scd2_utils.get_valid_from_sql(updated_at_col) }} as {{ valid_from_col }},
  {{ dbt_scd2_utils.get_valid_to_sql(unique_keys_csv, updated_at_col, none, deleted_at_col) }} as {{ valid_to_col }},
  {{ dbt_scd2_utils.get_change_type_sql(unique_keys_csv, updated_at_col, deleted_at_col) }} as {{ change_type_col }}
from changes_only

{% endmacro %}
