{# Builds the SCD2 delta temp relation without invoking contract enforcement.
   The model's YAML contract describes the final relation (with audit cols),
   but this temp holds only raw business columns — audit cols are added downstream
   in get_initial_load_scd2_sql and the merge SQL. Bypassing here keeps the
   final relation's contract intact while letting the temp build succeed. #}
{% macro create_temp_table_as(tmp_relation, compiled_code) %}
  {%- set sql_header = config.get('sql_header', none) -%}
  {{ sql_header if sql_header is not none }}
  create or replace temporary table {{ tmp_relation }} as (
    {{ compiled_code }}
  );
{% endmacro %}
