{#
  Shared planning logic for the SCD materializations.

  Resolves audit column configuration, builds the source temp relation, decides
  between an initial load and an incremental update, and returns the SQL to run.
  On the initial-load / full-refresh path, dbt_scd2_utils.get_full_refresh_sql picks
  truncate + insert (table metadata preserved) or create or replace.

  Both the generic `scd` materialization and the backwards-compatible
  `incremental_scd2` alias call this macro. dbt does not allow one materialization
  to invoke another, so the heavy lifting lives here and each materialization block
  stays a thin wrapper that executes the returned SQL and runs hooks.

  Args:
    sql (string): The compiled model SQL (passed from the materialization context).
    scd_type (int, optional): Forced SCD type. When none, resolved from config /
      vars, defaulting to 2.

  Returns:
    dict with keys:
      build_sql: The SQL statement to run as the model's main statement.
      target_relation: The target relation.
      tmp_relation: The temp relation to drop after the main statement runs.
#}

{% macro scd_plan(sql, scd_type=none) %}

  {%- set target_relation = this -%}
  {%- set existing_relation = load_relation(this) -%}
  {%- set tmp_relation = make_temp_relation(target_relation) -%}
  {%- set tmp_relation = tmp_relation.incorporate(type='table') -%}

  {# Resolve and validate the SCD type #}
  {%- if scd_type is none -%}
    {%- set scd_type = dbt_scd2_utils.get_config_value(config, 'scd_type', default=dbt_scd2_utils.get_from_object(var('dbt_scd2_utils', {}), 'scd_type', default=2)) -%}
  {%- endif -%}
  {%- set scd_type = scd_type | int -%}
  {%- if scd_type not in [0, 1, 2] -%}
    {{ exceptions.raise_compiler_error("scd_type must be 0, 1 or 2 for the scd materialization, got: " ~ scd_type) }}
  {%- endif -%}

  {# Get configurable audit column names #}
  {%- set is_current_col = dbt_scd2_utils.get_config_value(config, 'is_current_column', default=dbt_scd2_utils.get_from_object(var('dbt_scd2_utils', {}), 'is_current_column')) -%}
  {%- set valid_from_col = dbt_scd2_utils.get_config_value(config, 'valid_from_column', default=dbt_scd2_utils.get_from_object(var('dbt_scd2_utils', {}), 'valid_from_column')) -%}
  {%- set valid_to_col = dbt_scd2_utils.get_config_value(config, 'valid_to_column', default=dbt_scd2_utils.get_from_object(var('dbt_scd2_utils', {}), 'valid_to_column')) -%}
  {%- set updated_at_col = dbt_scd2_utils.get_config_value(config, 'updated_at_column', default=dbt_scd2_utils.get_from_object(var('dbt_scd2_utils', {}), 'updated_at_column')) -%}
  {%- set change_type_col = dbt_scd2_utils.get_config_value(config, 'change_type_column', default=dbt_scd2_utils.get_from_object(var('dbt_scd2_utils', {}), 'change_type_column')) -%}
  {%- set created_at_col = dbt_scd2_utils.get_config_value(config, 'created_at_column', default=dbt_scd2_utils.get_from_object(var('dbt_scd2_utils', {}), 'created_at_column', default=none)) -%}
  {%- set deleted_at_col = dbt_scd2_utils.get_config_value(config, 'deleted_at_column', default=dbt_scd2_utils.get_from_object(var('dbt_scd2_utils', {}), 'deleted_at_column', default=none)) -%}
  {%- set track_previous_version = dbt_scd2_utils.get_config_value(config, 'track_previous_version', default=dbt_scd2_utils.get_from_object(var('dbt_scd2_utils', {}), 'track_previous_version', default=false)) -%}
  {%- set previous_version_col = dbt_scd2_utils.get_config_value(config, 'previous_version_column', default=dbt_scd2_utils.get_from_object(var('dbt_scd2_utils', {}), 'previous_version_column', default='_PREVIOUS')) -%}
  {%- set track_changed_columns = dbt_scd2_utils.get_config_value(config, 'track_changed_columns', default=dbt_scd2_utils.get_from_object(var('dbt_scd2_utils', {}), 'track_changed_columns', default=false)) -%}
  {%- set changed_columns_col = dbt_scd2_utils.get_config_value(config, 'changed_columns_column', default=dbt_scd2_utils.get_from_object(var('dbt_scd2_utils', {}), 'changed_columns_column', default='_CHANGED')) -%}
  {%- set track_checksum = dbt_scd2_utils.get_config_value(config, 'track_checksum', default=dbt_scd2_utils.get_from_object(var('dbt_scd2_utils', {}), 'track_checksum', default=false)) -%}
  {%- set checksum_col = dbt_scd2_utils.get_config_value(config, 'checksum_column', default=dbt_scd2_utils.get_from_object(var('dbt_scd2_utils', {}), 'checksum_column', default='_CHECKSUM')) -%}
  {%- set checksum_exclude = dbt_scd2_utils.get_config_value(config, 'checksum_exclude', default=dbt_scd2_utils.get_from_object(var('dbt_scd2_utils', {}), 'checksum_exclude', default=[])) -%}

  {%- set unique_key = config.get('unique_key') -%}

  {%- if unique_key is none -%}
    {%- set error_message -%}
      You must provide a unique_key configuration for the scd materialization.
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

  {# Fail fast (before building the temp table): SCD types 0 and 1 keep a single #}
  {# row per key with no history, so there is nowhere to record a deletion.       #}
  {%- if scd_type in [0, 1] and deleted_at_col is not none -%}
    {%- set error_message -%}
      deleted_at_column ('{{ deleted_at_col }}') is set on an SCD type {{ scd_type }} model, but
      deletion tracking is not supported for SCD types 0 and 1. Either remove deleted_at_column or
      use scd_type=2.
    {%- endset -%}
    {{ exceptions.raise_compiler_error(error_message) }}
  {%- endif -%}

  {# _previous / _changed track version history, which only exists for SCD type 2. On #}
  {# types 0/1 we warn and omit the columns rather than error, so a folder-wide +meta #}
  {# switch does not break a layer that includes a type 0/1 model. The columns are #}
  {# appended only in the type-2 section below, so they are naturally not produced here. #}
  {%- if scd_type in [0, 1] and (track_previous_version or track_changed_columns) -%}
    {%- set warning_message -%}
      track_previous_version / track_changed_columns are set on an SCD type {{ scd_type }} model
      ({{ target_relation }}), but these columns are only produced for SCD type 2 (version history).
      They will be ignored for this model.
    {%- endset -%}
    {{ exceptions.warn(warning_message) }}
  {%- endif -%}

  {{ log("Building SCD Type " ~ scd_type ~ " table " ~ target_relation) }}

  {# Build the temp table with source data #}
  {%- call statement('build_temp_table') -%}
    {{ dbt_scd2_utils.create_temp_table_as(tmp_relation, sql) }}
  {%- endcall -%}

  {%- set dest_columns = adapter.get_columns_in_relation(tmp_relation) -%}

  {# Audit column names common to all SCD types #}
  {%- set audit_columns = [is_current_col, valid_from_col, valid_to_col, change_type_col] -%}

  {# Fail fast: a configured created_at / deleted_at column must actually be produced by #}
  {# the model, otherwise the generated SQL fails later with a cryptic 'invalid identifier'. #}
  {%- set dest_column_names_upper = dest_columns | map(attribute='name') | map('upper') | list -%}
  {%- for setting_name, setting_value in [('created_at_column', created_at_col), ('deleted_at_column', deleted_at_col)] -%}
    {%- if setting_value is not none and (setting_value | upper) not in dest_column_names_upper -%}
      {%- set error_message -%}
        {{ setting_name }} ('{{ setting_value }}') is configured but is not a column produced by
        this model ({{ target_relation }}). Add the column to the model or unset {{ setting_name }}.
        Available columns: {{ dest_column_names_upper | join(', ') }}
      {%- endset -%}
      {{ exceptions.raise_compiler_error(error_message) }}
    {%- endif -%}
  {%- endfor -%}

  {# Content checksum column set: business columns (including the natural key) minus the #}
  {# audit columns, the lifecycle columns, and any user-listed checksum_exclude columns, #}
  {# sorted so the fingerprint is independent of select-list order. Exclude volatile #}
  {# processing columns (e.g. an ingestion timestamp) here so identical business content #}
  {# yields a stable checksum. Shared by all SCD types; computed from the base audit #}
  {# columns before the type-2-only _previous / _changed are appended. #}
  {%- set checksum_lifecycle_cols = [] -%}
  {%- for c in [updated_at_col, created_at_col, deleted_at_col] -%}
    {%- if c is not none -%}{%- do checksum_lifecycle_cols.append(c) -%}{%- endif -%}
  {%- endfor -%}
  {%- set checksum_columns = dbt_scd2_utils.list_difference(dest_column_names_upper, dbt_scd2_utils.list_union(audit_columns, checksum_lifecycle_cols, (checksum_exclude or [])), case_insensitive=true) | sort -%}
  {%- if track_checksum -%}{%- do audit_columns.append(checksum_col) -%}{%- endif -%}

  {%- set should_full_refresh = (should_full_refresh() or existing_relation is none) -%}

  {# ------------------------------------------------------------------ #}
  {# SCD Types 0 and 1: one row per key. Type 0 never updates (original   #}
  {# value retained); Type 1 overwrites in place. Both forbid deletion    #}
  {# tracking and share a simple per-key argument dictionary.             #}
  {# ------------------------------------------------------------------ #}
  {%- if scd_type in [0, 1] -%}

    {%- set arg_dict = {
        'temp_relation': tmp_relation,
        'unique_key': unique_key,
        'dest_columns': dest_columns,
        'audit_columns': audit_columns,
        'is_current_column': is_current_col,
        'valid_from_column': valid_from_col,
        'valid_to_column': valid_to_col,
        'updated_at_column': updated_at_col,
        'change_type_column': change_type_col,
        'created_at_column': created_at_col,
        'track_checksum': track_checksum,
        'checksum_column': checksum_col,
        'checksum_columns': checksum_columns
    } -%}

    {%- if not should_full_refresh -%}
      {%- do arg_dict.update({'target_relation': target_relation}) -%}
    {%- endif -%}

    {%- if should_full_refresh -%}
      {{ log("Performing initial load for SCD" ~ scd_type ~ " table") }}
      {%- if scd_type == 0 -%}
        {%- set initial_load_sql = dbt_scd2_utils.get_initial_load_scd0_sql(arg_dict) -%}
      {%- else -%}
        {%- set initial_load_sql = dbt_scd2_utils.get_initial_load_scd1_sql(arg_dict) -%}
      {%- endif -%}
      {%- set build_sql = dbt_scd2_utils.get_full_refresh_sql(target_relation, existing_relation, dest_columns, audit_columns, initial_load_sql) -%}
    {%- else -%}
      {{ log("Performing incremental SCD" ~ scd_type ~ " update") }}
      {%- if scd_type == 0 -%}
        {%- set build_sql = dbt_scd2_utils.get_incremental_scd0_sql(arg_dict) -%}
      {%- else -%}
        {%- set build_sql = dbt_scd2_utils.get_incremental_scd1_sql(arg_dict) -%}
      {%- endif -%}
    {%- endif -%}

    {{ return({'build_sql': build_sql, 'target_relation': target_relation, 'tmp_relation': tmp_relation}) }}

  {%- endif -%}

  {# ------------------------------------------------------------------ #}
  {# SCD Type 2: full temporal history (the original behaviour).         #}
  {# ------------------------------------------------------------------ #}
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

  {# Collapse (delete) redundant versions that an out-of-order arrival removes from the #}
  {# timeline so incremental runs stay consistent with a full refresh. This needs the #}
  {# full prior history, so it is only applied when update_all_previous_records is true; #}
  {# otherwise it safely falls back to retaining (and re-expiring) the existing versions. #}
  {%- set collapse_redundant_versions = dbt_scd2_utils.get_from_object(var('dbt_scd2_utils', {}), 'collapse_redundant_versions', default=true) -%}
  {%- if collapse_redundant_versions and not update_all_previous_records -%}
    {{ exceptions.warn("dbt_scd2_utils: collapse_redundant_versions requires update_all_previous_records=true to be safe; redundant versions will be retained for " ~ this ~ ".") }}
  {%- endif -%}
  {%- set collapse_redundant_versions = collapse_redundant_versions and update_all_previous_records -%}

  {%- set merge_update_cols = [is_current_col, valid_to_col] -%}
  {# Recomputing the change column for every record ensures accuracy. #}
  {# No updating all previous records results in multiple 'I' records. #}
  {%- if update_all_previous_records -%}
    {%- do merge_update_cols.append(change_type_col) -%}
    {%- if track_previous_version -%}{%- do merge_update_cols.append(previous_version_col) -%}{%- endif -%}
    {%- if track_changed_columns -%}{%- do merge_update_cols.append(changed_columns_col) -%}{%- endif -%}
  {%- endif -%}

  {# Optional SCD2-only audit columns: prior-version object and per-column change map. #}
  {%- if track_previous_version -%}{%- do audit_columns.append(previous_version_col) -%}{%- endif -%}
  {%- if track_changed_columns -%}{%- do audit_columns.append(changed_columns_col) -%}{%- endif -%}

  {# New configuration approach with change_columns object #}
  {%- set change_columns_config = dbt_scd2_utils.get_config_value(config, 'change_columns', default=none) -%}

  {# Backwards compatibility: use old config if change_columns is not provided #}
  {%- if change_columns_config is not none -%}
    {%- set scd_check_columns_raw = dbt_scd2_utils.get_from_object(change_columns_config, 'include', default=none) -%}
    {%- set exclude_columns_from_change_check_raw = dbt_scd2_utils.get_from_object(change_columns_config, 'exclude', default=[]) -%}
    {%- set exclude_columns_from_change_check = (exclude_columns_from_change_check_raw or []) + [updated_at_col] -%}
  {%- else -%}
    {# Fall back to legacy configuration names #}
    {%- set scd_check_columns_raw = dbt_scd2_utils.get_config_value(config, 'scd_check_columns', default=none) -%}
    {%- set exclude_columns_from_change_check = dbt_scd2_utils.get_config_value(config, 'exclude_columns_from_change_check', default=[]) + [updated_at_col] -%}
  {%- endif -%}

  {%- set scd2_unique_key = unique_key + [updated_at_col] -%}

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
  {%- set suppress_date_type_warning = dbt_scd2_utils.get_from_object(var('dbt_scd2_utils', {}), 'suppress_date_type_warning', default=false) -%}
  {%- for column in dest_columns -%}
    {%- if column.name | upper == updated_at_col | upper -%}
      {%- set column_type = column.data_type | upper -%}
      {%- if 'DATE' in column_type and 'TIME' not in column_type and not suppress_date_type_warning -%}
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
      'created_at_column': created_at_col,
      'deleted_at_column': deleted_at_col,
      'track_previous_version': track_previous_version,
      'previous_version_column': previous_version_col,
      'track_changed_columns': track_changed_columns,
      'changed_columns_column': changed_columns_col,
      'track_checksum': track_checksum,
      'checksum_column': checksum_col,
      'checksum_columns': checksum_columns
  }  %}

  {%- if should_full_refresh -%}

    {# Initial load: create table with audit columns #}
    {{ log("Performing initial load for SCD2 table") }}

    {%- set initial_load_sql = dbt_scd2_utils.get_initial_load_scd2_sql(default_arg_dict) -%}

    {%- set build_sql = dbt_scd2_utils.get_full_refresh_sql(target_relation, existing_relation, dest_columns, audit_columns, initial_load_sql) -%}

  {%- else -%}

    {# Incremental load: use SCD2 merge logic #}
    {{ log("Performing incremental SCD2 update") }}

    {# Build the argument dictionary for the SCD2 SQL macro #}
    {%- do default_arg_dict.update({
      'target_relation': target_relation,
      'merge_update_cols': merge_update_cols,
      'incremental_predicates': config.get('incremental_predicates', []),
      'update_all_previous_records': update_all_previous_records,
      'collapse_redundant_versions': collapse_redundant_versions,
    }) -%}

    {%- set build_sql = dbt_scd2_utils.get_incremental_scd2_sql(default_arg_dict) -%}

  {%- endif -%}

  {{ return({'build_sql': build_sql, 'target_relation': target_relation, 'tmp_relation': tmp_relation}) }}

{% endmacro %}
