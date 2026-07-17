{#
  Canonicalise a value into a Snowflake-native UUID. The input may be either a
  32-character unhyphenated hex string (e.g. the md5 from
  dbt_utils.generate_surrogate_key) or an already-hyphenated UUID (or null). The
  hex case is reformatted to 8-4-4-4-12 first, then passed to Snowflake's
  `TO_UUID()` so the column emerges as the UUID type. Mirrors the
  envato-data-platform `to_uuid` macro that its surrogate keys and `_checksum`
  rely on; consumers should declare `data_type: uuid` on columns built from it.

  Args:
    column (string): A SQL expression yielding the value to canonicalise.

  Returns:
    A SQL expression (a `to_uuid(...)` call); no trailing alias.
#}

{%- macro to_uuid(column) -%}
    to_uuid(
        case
            when length({{ column }}) = 32 then
                substring({{ column }}, 1, 8) || '-' ||
                substring({{ column }}, 9, 4) || '-' ||
                substring({{ column }}, 13, 4) || '-' ||
                substring({{ column }}, 17, 4) || '-' ||
                substring({{ column }}, 21, 12)
            else {{ column }}::varchar
        end
    )
{%- endmacro -%}
