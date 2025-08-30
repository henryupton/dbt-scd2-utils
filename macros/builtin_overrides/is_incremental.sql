{%- docs is_incremental -%}
Override of dbt's built-in is_incremental() macro to support the incremental_scd2 materialization.

Checks if the current model is running in incremental mode by verifying:
1. The relation exists
2. The relation is a table
3. Not running in full refresh mode
4. The materialization is either 'incremental' or 'incremental_scd2'

**Returns:**
- Boolean indicating if the model should run in incremental mode

**Note:**
This override extends dbt's standard incremental logic to recognize the custom
'incremental_scd2' materialization type alongside the standard 'incremental'.
{%- enddocs -%}

{% macro is_incremental() %}
  {#-- do not run introspective queries in parsing #}
  {%- if not execute -%}
    {{ return(False) }}
  {%- else -%}
    {%- set relation = load_relation(this) -%}
    {{ return(relation is not none 
              and relation.type == 'table' 
              and not should_full_refresh()
              and (config.get('materialized') in ('incremental', 'incremental_scd2'))) }}
  {%- endif -%}
{%- endmacro -%}