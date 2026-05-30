{#
  Asserts that a model's rows (projected to compare_columns) exactly match an
  expected seed, as a symmetric set difference. The expected seed is chosen by the
  current `iteration` var: <seed_prefix>_<iteration>. This lets the same test cover
  the initial load (iteration 1) and successive incremental loads (iteration 2..n)
  when driven by test_scd2_sequence.sh.

  Fails if the model has any row the seed doesn't, or vice versa.
#}

{% test matches_expected_seed(model, seed_prefix, compare_columns) %}

  {%- set iteration = var('iteration', 1) -%}
  {%- set expected = ref(seed_prefix ~ '_' ~ iteration) -%}
  {%- set cols_csv = dbt_scd2_utils.get_quoted_csv(compare_columns) -%}

  with
  actual as (
      select {{ cols_csv }} from {{ model }}
  ),
  expected as (
      select {{ cols_csv }} from {{ expected }}
  ),
  unexpected_in_actual as (
      select {{ cols_csv }} from actual
      except
      select {{ cols_csv }} from expected
  ),
  missing_from_actual as (
      select {{ cols_csv }} from expected
      except
      select {{ cols_csv }} from actual
  )
  select 'unexpected_in_actual' as _problem, {{ cols_csv }} from unexpected_in_actual
  union all
  select 'missing_from_actual' as _problem, {{ cols_csv }} from missing_from_actual

{% endtest %}
