{#
  Shared source rows for the fingerprint fixtures. Vars, all optional:

    fp_source                 which seed: base | appended | versioned | inplace | backdated | deleted | deleted_b
    fp_upper_email            a logic change below the watermark
    fp_null_email_customer    a value flipped to null for one customer
    fp_extra_column           a shape change (adds extra_col)
    fp_drop_column            a shape change (drops customer_name)
    fp_duplicate_rows         every row twice, a logic bug that hash_agg must see
    fp_bump_loaded_at         every row's _loaded_at set to now, a watermark game
    fp_shuffle                random row order, which must not register as a change

  Its own var names throughout, because the other suites key their seeds off `iteration`.
#}
{% macro fp_customer_rows() %}
  {%- set source = var('fp_source', 'base') -%}
  {%- set null_customer = var('fp_null_email_customer', none) -%}
  {%- set email_expr = 'upper(email)' if var('fp_upper_email', false) else 'email' -%}
  {%- if null_customer is not none -%}
    {%- set email_expr = 'case when customer_id = ' ~ (null_customer | int) ~ ' then null else ' ~ email_expr ~ ' end' -%}
  {%- endif -%}
  with src as (
      select
          customer_id,
          customer_name,
          email,
          status,
          _updated_at::timestamp_tz as _updated_at,
          _loaded_at::timestamp_tz as _loaded_at
      from {{ ref('fp_raw_' ~ source) }}
      {% if var('fp_duplicate_rows', false) %}
      union all
      select
          customer_id,
          customer_name,
          email,
          status,
          _updated_at::timestamp_tz,
          _loaded_at::timestamp_tz
      from {{ ref('fp_raw_' ~ source) }}
      {% endif %}
  )
  select
      customer_id,
      {% if not var('fp_drop_column', false) %}customer_name,{% endif %}
      {{ email_expr }} as email,
      status,
      {% if var('fp_extra_column', false) %}'x' as extra_col,{% endif %}
      _updated_at,
      {% if var('fp_bump_loaded_at', false) %}current_timestamp()::timestamp_tz{% else %}_loaded_at{% endif %} as _loaded_at
  from src
  {% if var('fp_shuffle', false) %}order by random(){% endif %}
{% endmacro %}
