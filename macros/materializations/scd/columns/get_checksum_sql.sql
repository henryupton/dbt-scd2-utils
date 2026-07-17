{#
  Builds the expression for the optional `_checksum` audit column: an md5 content
  fingerprint of the model's business columns, via dbt_utils.generate_surrogate_key (the
  same function envato-data-platform's generate_checksum wraps). The caller passes the
  already-sorted checksum_columns (business columns including the natural key, minus the
  audit and lifecycle columns).

  Args:
    checksum_columns (list): Business columns to fingerprint, pre-sorted.

  Returns:
    A SQL expression (generate_surrogate_key call); no trailing alias.
#}

{%- macro get_checksum_sql(checksum_columns) -%}
{{ dbt_utils.generate_surrogate_key(checksum_columns) }}
{%- endmacro -%}
