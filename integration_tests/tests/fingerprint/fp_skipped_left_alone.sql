{{
    config(
        tags=['fingerprint', 'fp_overwrite', 'fp_evolve', 'fp_guard', 'fp_late', 'fp_edp', 'fp_edge']
    )
}}

-- depends_on: {{ ref('fp_ledger') }}

{# A skipped node's table was left alone: no row in it was committed at or after this deploy's snapshot. #}
{%- set checks = [] -%}
{%- if execute -%}
  {%- set node_rel = dbt_scd2_utils.fingerprint_relation('deploy_node') -%}
  {%- set res = run_query(
      "select relation_name, to_char(snapshot_at, 'YYYY-MM-DD HH24:MI:SS.FF9 TZHTZM')"
      ~ " from (select * from " ~ node_rel
      ~ " where deploy_id = " ~ dbt_scd2_utils.fingerprint_lit(var('deploy_id', invocation_id))
      ~ dbt_scd2_utils.fingerprint_latest_row() ~ ") where verdict = 'skipped'") -%}
  {%- for row in res.rows -%}
    {%- do checks.append(
        "select '" ~ row[0] ~ "' as relation_name, max(metadata$row_last_commit_time) as last_commit from " ~ row[0]
        ~ " having max(metadata$row_last_commit_time) >= '" ~ row[1] ~ "'::timestamp_tz") -%}
  {%- endfor -%}
{%- endif -%}
{%- if checks | length == 0 %}
select null as relation_name, null as last_commit where false
{%- else %}
{{ checks | join('\nunion all\n') }}
{%- endif %}
