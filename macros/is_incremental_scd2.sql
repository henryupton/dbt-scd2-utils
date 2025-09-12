{#
    Checks if the current model is running in incremental SCD2 mode by verifying:
    1. The relation exists
    2. The relation is a table
    3. Not running in full refresh mode
    4. The materialization is specifically 'incremental_scd2'

    This is similar to the built-in is_incremental() macro but only returns True for
    incremental_scd2 materializations, not standard incremental materializations.

    **Returns:**
    - Boolean indicating if the model should run in incremental SCD2 mode

    **Example:**
    ```sql
    {% if is_incremental_scd2() %}
      -- SCD2-specific incremental logic here
    {% endif %}
    ```
#}

{% macro is_incremental_scd2() %}
  {#-- do not run introspective queries in parsing #}
  {%- if not execute -%}
    {{ return(False) }}
  {%- else -%}
    {%- set relation = load_relation(this) -%}
    {{ return(relation is not none 
              and relation.type == 'table' 
              and not should_full_refresh()
              and config.get('materialized') == 'incremental_scd2') }}
  {%- endif -%}
{%- endmacro -%}