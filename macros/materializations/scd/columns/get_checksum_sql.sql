{#
  Builds the expression for the optional `_checksum` audit column: a native-UUID content
  fingerprint of the model's business columns. The md5 from dbt_utils.generate_surrogate_key
  is reformatted to 8-4-4-4-12 and cast to a UUID via to_uuid, exactly as
  envato-data-platform's generate_checksum does (its overridden generate_surrogate_key wraps
  the same md5 in to_uuid). The caller passes the already-sorted checksum_columns (business
  columns including the natural key, minus the audit and lifecycle columns).

  Args:
    checksum_columns (list): Business columns to fingerprint, pre-sorted.

  Returns:
    A SQL expression (a to_uuid(...) call yielding the UUID type); no trailing alias.
#}

{%- macro get_checksum_sql(checksum_columns) -%}
{{ dbt_scd2_utils.to_uuid(dbt_utils.generate_surrogate_key(checksum_columns)) }}
{%- endmacro -%}
