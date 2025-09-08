{%- docs get_incremental_scd2_sql -%}
Generates the SQL for incremental SCD Type 2 processing using a MERGE statement.

This macro creates a complex MERGE statement that handles SCD Type 2 logic by:
1. Identifying new/changed records from the source
2. Comparing them with existing records using hash-based change detection
3. Properly setting SCD audit columns (is_current, valid_from, valid_to, etc.)
4. Updating expired records and inserting new versions

**Args:**
- `arg_dict` (dict): Configuration dictionary containing:
  - `target_relation`: The target table to merge into
  - `temp_relation`: Temporary table with new data
  - `unique_key`: Array of business key columns
  - `dest_columns`: Existing table column metadata
  - `incremental_predicates`: Optional filtering predicates
  - `is_current_column`: Name of the is_current flag column
  - `valid_from_column`: Name of the valid_from timestamp column
  - `valid_to_column`: Name of the valid_to timestamp column
  - `updated_at_column`: Name of the updated_at timestamp column
  - `change_type_column`: Name of the change_type column
  - `change_type_expr`: Optional custom expression for change_type
  - `scd_check_columns`: Columns to include in change detection hash
  - `default_valid_to`: Default timestamp for current records

**Returns:**
- Complete MERGE SQL statement for SCD Type 2 incremental processing

**Example:**
The generated MERGE will handle scenarios like:
- New customer record → INSERT with is_current=true
- Changed customer data → UPDATE old record to is_current=false, valid_to to next valid_from, INSERT new version
- Unchanged customer data → No action (filtered out by hash comparison)
{%- enddocs -%}

{% macro get_incremental_scd2_sql(arg_dict) %}
    {% set target_relation = arg_dict["target_relation"] %}
    {% set temp_relation = arg_dict["temp_relation"] %}
    {% set unique_key = arg_dict["unique_key"] %}
    {% set dest_columns = arg_dict["dest_columns"] %}
    {% set incremental_predicates = arg_dict.get("incremental_predicates", []) %}

    {# Define our audit columns – these are crucial for SCD2 tracking. These need to be present in the table already too.#}
    {%- set is_current_col = arg_dict.get('is_current_column', var('is_current_column', '_IS_CURRENT')) -%}
    {%- set valid_from_col = arg_dict.get('valid_from_column', var('valid_from_column', '_VALID_FROM')) -%}
    {%- set valid_to_col = arg_dict.get('valid_to_column', var('valid_to_column', '_VALID_TO')) -%}
    {%- set updated_at_col = arg_dict.get('updated_at_column', var('updated_at_column', '_UPDATED_AT')) -%}
    {%- set change_type_col = arg_dict.get('change_type_column', var('change_type_column', '_CHANGE_TYPE')) -%}
    {%- set created_at_col = arg_dict.get('created_at_column', var('created_at_column', '_CREATED_AT')) -%}
    {%- set scd_check_columns = arg_dict.get('scd_check_columns', none) -%}
    {%- set change_type_expr = arg_dict.get('change_type_expr', none) -%}
    {%- set default_valid_to = arg_dict.get('default_valid_to', var('default_valid_to', '2999-12-31 23:59:59')) -%}
    {%- set audit_cols_names = [is_current_col, valid_from_col, valid_to_col, updated_at_col, change_type_col, created_at_col] -%}

    {%- set updated_at = '"' + updated_at_col + '"' -%}

    {%- set unique_key_all = unique_key + [updated_at_col] -%}

    {# Prepare column lists for the MERGE statement #}
    {%- set unique_keys_csv = dbt_scd2_utils.get_quoted_csv(unique_key | map("upper")) -%}
    {%- set dest_cols_names = dest_columns | map(attribute="name") | map("upper") | reject('in', audit_cols_names) | list -%}
    {%- set dest_cols_csv = dbt_scd2_utils.get_quoted_csv(dest_cols_names) -%}
    {%- set all_cols_names = dest_cols_names + audit_cols_names -%}
    {%- set all_cols_csv = dbt_scd2_utils.get_quoted_csv(all_cols_names) -%}

    {%- set merge_update_cols = [is_current_col, valid_to_col] -%}

    {# Process change_type_expr - defaults to change_type_col if not provided #}
    {%- if change_type_expr -%}
        {%- set change_type_sql = change_type_expr -%}
    {%- else -%}
        {# Default ROW_NUMBER logic #}
        {%- set change_type_sql = dbt_scd2_utils.get_change_type_sql(unique_keys_csv, updated_at) -%}
    {%- endif -%}

    {# Build hash-based change detection #}
    {%- if scd_check_columns -%}
        {%- set hash_columns = scd_check_columns -%}
    {%- else -%}
        {# If no scd_check_columns specified, use all business columns #}
        {%- set hash_columns = dest_cols_names -%}
    {%- endif -%}

{# This section is where the magic happens: the MERGE statement #}
merge into {{ target_relation }} AS DBT_INTERNAL_DEST
using (
    with
        {# New records are those coming from our current run, based on the model logic and run mode. #}
        new_records as (
            select
                {{ dest_cols_csv }},
                'new' as _source,
                17 as _priority,
                {{ updated_at }},
                {{ dbt_utils.generate_surrogate_key(unique_key_all) }} as _scd2_key,
                {{ dbt_utils.generate_surrogate_key(hash_columns | list) }} as _scd2_hash,
            from {{ temp_relation }}
        )
        -- select * from new_records order by {{ unique_keys_csv }}, {{ updated_at }} limit 137;
        ,
        {# We need the existing version of any records that are about to be updated #}
        previous_record as (
            select
                {{ dbt_scd2_utils.get_quoted_csv(dest_cols_names, 'p.') }},
                'previous' as _source,
                0 as _priority,
                p.{{ updated_at }},
                {{ dbt_utils.generate_surrogate_key(dbt_scd2_utils.prefix_array_elements(unique_key_all, 'p.')) }} as _scd2_key,
                {{ dbt_utils.generate_surrogate_key(dbt_scd2_utils.prefix_array_elements(hash_columns, 'p.')) }} as _scd2_hash,
            from {{ this }} as p
            inner join new_records as n on {% for col in unique_key -%}
                p.{{ col }} = n.{{ col }} {% if not loop.last %} and {% endif %}
            {%- endfor %}
            {# We want all previous records which could have been valid when any of the new records occurred. #}
            where n.{{ updated_at }} <= p.{{ valid_to_col }} -- Only those that could be affected by the new record's updated_at.
        )
        -- select * from previous_record order by {{ unique_keys_csv }}, {{ updated_at }} limit 213;
        ,

        {# Bring the band together. #}
        all_records as (
            select 
                {% for col in dest_cols_names -%}
                {{ col }},
                {%- endfor %}
                _source,
                _priority,
                {{ updated_at }},
                _scd2_key,
                _scd2_hash,
            from new_records
            
            union all
            
            select 
                {% for col in dest_cols_names -%}
                {{ col }},
                {%- endfor %}
                _source,
                _priority,
                {{ updated_at }},
                _scd2_key,
                _scd2_hash,
            from previous_record
        )
        -- select * from all_records {{ unique_keys_csv }}, {{ updated_at }} limit 321;
        ,

        {# Make sure we have only one record for each unique key, updated_at permutation. #}
        {# Prioritise existing record over a new one in the case of a duplicate. Why would something have changed but not produced a new updated_at? #}
        distinct_records as (
            select
                *
            from all_records
            qualify row_number() over(partition by _scd2_key order by _priority) = 1
        )
        -- select * from distinct_records order by {{ unique_keys_csv }}, {{ updated_at }} limit 123;

    select
        {{ dest_cols_csv }},
        {# SCD2 audit columns using reusable macros #}
        {{ dbt_scd2_utils.get_is_current_sql(unique_keys_csv, updated_at) }} as {{ is_current_col }},
        {{ dbt_scd2_utils.get_valid_from_sql(updated_at) }} as {{ valid_from_col }},
        {{ dbt_scd2_utils.get_valid_to_sql(unique_keys_csv, updated_at, default_valid_to) }} as {{ valid_to_col }},
        {{ updated_at }} as {{ updated_at_col }},
        {{ dbt_scd2_utils.get_change_type_sql(unique_keys_csv, updated_at) }} as {{ change_type_col }},
        {{ dbt_scd2_utils.get_created_at_sql(unique_keys_csv, updated_at) }} as {{ created_at_col }},
    from distinct_records
    ) AS DBT_INTERNAL_SOURCE
on (
    {# Matching condition for the MERGE: unique key and the updated_at timestamp #}
    {% for col in unique_key_all -%}
        DBT_INTERNAL_DEST.{{ col }} = DBT_INTERNAL_SOURCE.{{ col }}{% if not loop.last %} and {% endif %}
    {%- endfor %}
    {%- if incremental_predicates -%}
    {# Optional: Incremental Predicates (if defined in dbt_project.yml or model config) #}
    and (
        {% for predicate in incremental_predicates %}
            {{ predicate }}
            {% if not loop.last %} AND {% endif %}
        {% endfor %}
        )
    {%- endif -%}
)
{# When a match is found, we update the existing record (this typically happens to set _is_current to false or _valid_to for old records) #}
when matched then update set
    {% for col in merge_update_cols %}
        DBT_INTERNAL_DEST.{{ col }} = DBT_INTERNAL_SOURCE.{{ col }}{% if not loop.last %},{% endif %}
    {%- endfor %}
{# When no match is found, it's a new record or a new version of an existing record, so we insert it #}
when not matched then insert ({{ all_cols_csv }})
values ({{ all_cols_csv }})
{% endmacro %}
