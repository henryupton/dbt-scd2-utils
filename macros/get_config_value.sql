{#
  Gets a configuration value from the model config, checking meta block first, then top-level config.
  This provides backwards compatibility for DBT Fusion which requires custom config in meta block.

  This is a stop gap, dbt Fusion will surface meta args as top level configs in the near future.

  Args:
    config: The dbt config object
    key (str): The configuration key to look up
    default (any, optional): Value to return if key not found. Defaults to none.

  Returns:
    The configuration value from meta or config, or default if not found

  Examples:
    {% set is_current_col = dbt_scd2_utils.get_config_value(config, 'is_current_column') %}

    {% set change_cols = dbt_scd2_utils.get_config_value(config, 'change_columns', default={}) %}
#}

{% macro get_config_value(config, key) %}
  {%- set default = kwargs.get('default', none) -%}

  {# First check meta block via meta_get (avoids the dbt deprecation warning fired by config.get for custom keys). #}
  {%- set meta_value = config.meta_get(key) -%}
  {%- if meta_value is not none -%}
    {{ return(meta_value) }}
  {%- endif -%}

  {# Fall back to top-level config for users still declaring custom keys outside of meta. #}
  {{ return(config.get(key, default)) }}
{% endmacro %}