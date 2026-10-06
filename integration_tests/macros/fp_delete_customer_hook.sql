{#
  Test-only pre-hook: when fp_delete_customer is set, delete that customer's rows from the model
  being built, after the deploy's snapshot and before its main statement. The build then sees a
  deletion in a month it may not otherwise touch. No-op otherwise.

    pre_hook="{{ fp_delete_customer_hook() }}"
#}
{% macro fp_delete_customer_hook() %}
  {%- if not execute or var('fp_delete_customer', none) is none -%}
    {{ return('select 1 where false') }}
  {%- endif -%}
  {{ return("delete from " ~ this ~ " where customer_id = " ~ (var('fp_delete_customer') | int)) }}
{% endmacro %}
