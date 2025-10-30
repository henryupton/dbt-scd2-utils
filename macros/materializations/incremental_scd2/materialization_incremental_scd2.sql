{% materialization incremental_scd2, default %}

  {%- set target_relation = this -%}
  {%- set existing_relation = load_relation(this) -%}
  {%- set tmp_relation = make_temp_relation(target_relation) -%}
  {%- set tmp_relation = tmp_relation.incorporate(type='table') -%}

  {% set incremental_predicates = config.get("incremental_predicates", []) %}

  {# Get configurable audit column names #}
  {%- set is_current_col = config.get('is_current_column', dbt_scd2_utils.get_from_object(var('dbt_scd2_utils', {}), 'is_current_column')) -%}
  {%- set valid_from_col = config.get('valid_from_column', dbt_scd2_utils.get_from_object(var('dbt_scd2_utils', {}), 'valid_from_column')) -%}
  {%- set valid_to_col = config.get('valid_to_column', dbt_scd2_utils.get_from_object(var('dbt_scd2_utils', {}), 'valid_to_column')) -%}
  {%- set updated_at_col = config.get('updated_at_column', dbt_scd2_utils.get_from_object(var('dbt_scd2_utils', {}), 'updated_at_column')) -%}
  {%- set change_type_col = config.get('change_type_column', dbt_scd2_utils.get_from_object(var('dbt_scd2_utils', {}), 'change_type_column')) -%}
  {%- set deleted_at_col = config.get('deleted_at_column', dbt_scd2_utils.get_from_object(var('dbt_scd2_utils', {}), 'deleted_at_column', default=none)) -%}

  {%- set update_all_previous_records = dbt_scd2_utils.get_from_object(var('dbt_scd2_utils', {}), 'update_all_previous_records', default=true) -%}

  {%- if not update_all_previous_records -%}
    {%- set warning_message -%}
      update_all_previous_records is set to false for {{ this }}.

      This is a performance optimization that reduces the number of records updated during incremental runs.
      However, this setting assumes that no new data will arrive with timestamps that predate the earliest
      record for a given key (i.e., no "backfill" data).

      If backfill data does arrive, it will result in undocumented behavior in the {{ change_type_col }} column,
      potentially causing multiple records to be marked as 'I' (INSERT) for the same key.

      Only use this setting if you can guarantee that all data arrives in chronological order.
    {%- endset -%}
    {{ exceptions.warn(warning_message) }}
  {%- endif -%}

  {%- set merge_update_cols = [is_current_col, valid_to_col] -%}
  {# Recomputing the change column for every record ensures accuracy. #}
  {# No updating all previous records results in multiple 'I' records. #}
  {%- if update_all_previous_records -%}
    {%- do merge_update_cols.append(change_type_col) -%}
  {%- endif -%}

  {# New configuration approach with change_columns object #}
  {%- set change_columns_config = config.get('change_columns', none) -%}

  {# Backwards compatibility: use old config if change_columns is not provided #}
  {%- if change_columns_config is not none -%}
    {%- set scd_check_columns_raw = dbt_scd2_utils.get_from_object(change_columns_config, 'include', none) -%}
    {%- set exclude_columns_from_change_check = dbt_scd2_utils.get_from_object(change_columns_config, 'exclude', []) + [updated_at_col] -%}
  {%- else -%}
    {# Fall back to legacy configuration names #}
    {%- set scd_check_columns_raw = config.get('scd_check_columns', none) -%}
    {%- set exclude_columns_from_change_check = config.get('exclude_columns_from_change_check', []) + [updated_at_col] -%}
  {%- endif -%}

  {%- set unique_key = config.get('unique_key') -%}
  {%- set scd2_unique_key = unique_key + [updated_at_col] -%}

  {%- if unique_key is none -%}
    {%- set error_message -%}
      You must provide a unique_key configuration for incremental_scd2 materialization.
      This should be the business key (natural key) of the dimension.
    {%- endset -%}
    {{ exceptions.raise_compiler_error(error_message) }}
  {%- endif -%}

  {%- if not dbt_scd2_utils.is_array(unique_key) -%}
    {%- set error_message -%}
      The unique_key configuration must be an array of column names.
      Received: {{ unique_key }} ({{ unique_key.__class__.__name__ }})
    {%- endset -%}
    {{ exceptions.raise_compiler_error(error_message) }}
  {%- endif -%}

  {{ log("Building SCD Type 2 table " ~ target_relation) }}

  {# Build the temp table with source data #}
  {%- call statement('build_temp_table') -%}
    {{ create_table_as(True, tmp_relation, sql) }}
  {%- endcall -%}

  {%- set dest_columns = adapter.get_columns_in_relation(tmp_relation) -%}
  
  {# Get audit column names #}
  {%- set audit_columns = [is_current_col, valid_from_col, valid_to_col, change_type_col] -%}

  {# Process scd_check_columns with guard pattern and filtering #}
  {%- if scd_check_columns_raw is not none -%}
    {# Get case insensitive overlap with dest_columns #}
    {%- set dest_column_names = dest_columns | map(attribute='name') | list -%}
    {%- set scd_check_columns = dbt_scd2_utils.list_intersection(scd_check_columns_raw, dest_column_names, case_insensitive=true) -%}
    
    {# Create union of all columns to exclude #}
    {%- set exclude_columns = dbt_scd2_utils.list_union(exclude_columns_from_change_check, unique_key, audit_columns) -%}
    
    {# Remove all excluded columns #}
    {%- set scd_check_columns = dbt_scd2_utils.list_difference(scd_check_columns, exclude_columns, case_insensitive=true) -%}
  {%- else -%}
    {# If scd_check_columns is not specified, default to all non-excluded columns #}
    {%- set dest_column_names = dest_columns | map(attribute='name') | list -%}
    {%- set exclude_columns = dbt_scd2_utils.list_union(exclude_columns_from_change_check, unique_key, audit_columns) -%}
    
    {%- set scd_check_columns = dbt_scd2_utils.list_difference(dest_column_names, exclude_columns, case_insensitive=true) -%}
  {%- endif -%}

  {# Validate updated_at column type #}
  {%- for column in dest_columns -%}
    {%- if column.name | upper == updated_at_col | upper -%}
      {%- set column_type = column.data_type | upper -%}
      {%- if 'DATE' in column_type and 'TIME' not in column_type -%}
        {%- set warning_message -%}
          Column '{{ updated_at_col }}' has type '{{ column_type }}' which is a DATE type.
          SCD2 logic works best with TIMESTAMP types for precise change tracking.
          Consider using a TIMESTAMP column for more accurate validity windows.
          Undocumented behavior may occur when using DATE types.
        {%- endset -%}
        {{ exceptions.warn(warning_message) }}
      {%- endif -%}
    {%- endif -%}
  {%- endfor -%}

  {%- set should_full_refresh = (should_full_refresh() or existing_relation is none) -%}

  {% set default_arg_dict = {
      'temp_relation': tmp_relation,
      'unique_key': unique_key,
      'scd2_unique_key': scd2_unique_key,

      'dest_columns': dest_columns,
      'scd_check_columns': scd_check_columns,
      'audit_columns': audit_columns,

      'is_current_column': is_current_col,
      'valid_from_column': valid_from_col,
      'valid_to_column': valid_to_col,
      'updated_at_column': updated_at_col,
      'change_type_column': change_type_col,
      'deleted_at_column': deleted_at_col
  }  %}

  {%- if should_full_refresh -%}
    
    {# Initial load: create table with audit columns #}
    {{ log("Performing initial load for SCD2 table") }}
    
    {%- set initial_load_sql = dbt_scd2_utils.get_initial_load_scd2_sql(default_arg_dict) -%}

    {%- set build_sql = get_create_table_as_sql(False, target_relation, initial_load_sql) -%}

  {%- else -%}
    
    {# Incremental load: use SCD2 merge logic #}
    {{ log("Performing incremental SCD2 update") }}

    {# Build the argument dictionary for the SCD2 SQL macro #}
    {%- do default_arg_dict.update({
      'target_relation': target_relation,
      'merge_update_cols': merge_update_cols,
      'incremental_predicates': config.get('incremental_predicates', []),
      'update_all_previous_records': update_all_previous_records,
    }) -%}

    {%- set build_sql = dbt_scd2_utils.get_incremental_scd2_sql(default_arg_dict) -%}

  {%- endif -%}

  {%- call statement('main') -%}
    {{ build_sql }}
  {%- endcall -%}

  {{ run_hooks(post_hooks, inside_transaction=True) }}

  {# Drop any temp tables we've created along the way. #}
  {%- if tmp_relation is not none -%}
    {%- do adapter.drop_relation(tmp_relation) -%}
  {%- endif -%}

  {%- set target_relation = target_relation.incorporate(type='table') -%}

  {% do persist_docs(target_relation, model) %}

  {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}
