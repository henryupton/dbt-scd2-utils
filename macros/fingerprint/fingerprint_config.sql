{#
  Content fingerprint: shared settings.

  Vars (all top-level, so they can be set in dbt_project.yml or with --vars):
    fingerprint (bool):                          master switch, default false.
    fingerprint_skip_unchanged_upstream (bool):  lets fingerprint_should_skip() answer true, default false.
    deploy_id (string):                          groups one deploy's ledger rows, default invocation_id.
    fingerprint_schema (string):                 ledger schema, default target.schema.
    fingerprint_loaded_at_column (string):       row arrival column, default _loaded_at.
    fingerprint_exclude (list):                  columns never hashed, default [_batched_at, _written_at, _synthesised_at].
      The scd validity columns (is_current_column, valid_to_column) are always excluded as well.

  Per-model meta: fingerprint_loaded_at, fingerprint_exclude (added to the global list).
#}

{% macro fingerprint_enabled() %}
  {{ return(var('fingerprint', false) | as_bool) }}
{% endmacro %}

{% macro fingerprint_skip_enabled() %}
  {{ return(var('fingerprint_skip_unchanged_upstream', false) | as_bool) }}
{% endmacro %}

{% macro fingerprint_deploy_id() %}
  {{ return(var('deploy_id', invocation_id)) }}
{% endmacro %}

{% macro fingerprint_schema() %}
  {{ return(var('fingerprint_schema', target.schema)) }}
{% endmacro %}

{% macro fingerprint_relation(name) %}
  {{ return(api.Relation.create(database=target.database, schema=dbt_scd2_utils.fingerprint_schema(), identifier='fingerprint_' ~ name, type='table')) }}
{% endmacro %}

{% macro fingerprint_node_meta(node) %}
  {%- if node is none or node.config is not defined or node.config is none -%}
    {{ return({}) }}
  {%- endif -%}
  {%- if node.config.meta is defined and node.config.meta is not none -%}
    {{ return(node.config.meta) }}
  {%- endif -%}
  {{ return({}) }}
{% endmacro %}

{% macro fingerprint_loaded_at_column(node) %}
  {%- set meta = dbt_scd2_utils.fingerprint_node_meta(node) -%}
  {{ return(meta.get('fingerprint_loaded_at') or var('fingerprint_loaded_at_column', '_loaded_at')) }}
{% endmacro %}

{% macro fingerprint_excluded_columns(node) %}
  {%- set pkg = var('dbt_scd2_utils', {}) -%}
  {%- set cols = [] -%}
  {%- for c in var('fingerprint_exclude', ['_batched_at', '_written_at', '_synthesised_at']) -%}{%- do cols.append(c) -%}{%- endfor -%}
  {%- do cols.append(dbt_scd2_utils.get_from_object(pkg, 'is_current_column', default='_is_current')) -%}
  {%- do cols.append(dbt_scd2_utils.get_from_object(pkg, 'valid_to_column', default='_valid_to')) -%}
  {%- for c in dbt_scd2_utils.fingerprint_node_meta(node).get('fingerprint_exclude', []) or [] -%}{%- do cols.append(c) -%}{%- endfor -%}
  {{ return(cols | map('upper') | list) }}
{% endmacro %}

{% macro fingerprint_blocking_verdicts() %}
  {{ return(['pending', 'new', 'modified', 'unhashable', 'error']) }}
{% endmacro %}

{# SQL literal helpers: none -> null, strings quoted and escaped, everything else as-is. #}
{% macro fingerprint_lit(value) %}
  {%- if value is none -%}
    {{ return('null') }}
  {%- elif value is string -%}
    {{ return("'" ~ (value | replace("'", "''")) ~ "'") }}
  {%- elif value is sameas true -%}
    {{ return('true') }}
  {%- elif value is sameas false -%}
    {{ return('false') }}
  {%- else -%}
    {{ return(value | string) }}
  {%- endif -%}
{% endmacro %}

{% macro fingerprint_ts_lit(value) %}
  {%- if value is none -%}{{ return('null') }}{%- endif -%}
  {{ return("'" ~ value ~ "'::timestamp_tz") }}
{% endmacro %}

{#
  Column shapes from a `show columns in table|schema` result: one entry per table, "NAME:{type json},..." ordered
  by column name, so a reorder is not a change but an added, removed or retyped column is. SHOW is metadata-only
  (about 0.1 s per table, a few seconds per schema) where information_schema.columns cost 7 to 10 s per lookup on
  the production account. Both hooks read the same source so the strings compare byte for byte.
#}
{% macro fingerprint_shape_from_show(res) %}
  {%- set idx = {} -%}
  {%- for name in res.columns | map(attribute='name') -%}{%- do idx.update({name | lower: loop.index0}) -%}{%- endfor -%}
  {%- set by_table = {} -%}
  {%- for row in res.rows -%}
    {%- set t = row[idx['table_name']] | upper -%}
    {%- if t not in by_table -%}{%- do by_table.update({t: []}) -%}{%- endif -%}
    {%- do by_table[t].append((row[idx['column_name']] | upper) ~ ':' ~ row[idx['data_type']]) -%}
  {%- endfor -%}
  {%- set shapes = {} -%}
  {%- for t, cols in by_table.items() -%}
    {%- do shapes.update({t: cols | sort | join(',')}) -%}
  {%- endfor -%}
  {{ return(shapes) }}
{% endmacro %}

{# Column names (upper) from the same SHOW result, so the post-hook needs no separate describe. #}
{% macro fingerprint_columns_from_show(res) %}
  {%- set idx = {} -%}
  {%- for name in res.columns | map(attribute='name') -%}{%- do idx.update({name | lower: loop.index0}) -%}{%- endfor -%}
  {%- set cols = [] -%}
  {%- for row in res.rows -%}{%- do cols.append(row[idx['column_name']] | upper) -%}{%- endfor -%}
  {{ return(cols) }}
{% endmacro %}

{#
  Object state without information_schema.tables (2 s per lookup): `show tables like` is metadata-only, and
  result_scan on it settles created_on against the snapshot in SQL. LIKE treats `_` as a wildcard, so the exact
  name is re-checked. Returns {'found', 'kind', 'replaced', 'retention'} with kind 'table', 'view' or none.
#}
{% macro fingerprint_object_info(relation, snap) %}
  {%- set name_lit = dbt_scd2_utils.fingerprint_lit(relation.identifier) -%}
  {%- set in_schema = " in schema " ~ relation.database ~ "." ~ relation.schema -%}
  {%- set tables = run_query("show tables like " ~ name_lit ~ in_schema) -%}
  {%- if tables.rows | length > 0 -%}
    {%- set info = run_query(
        "select \"kind\", \"retention_time\", \"created_on\" > " ~ snap ~ " as replaced"
        ~ " from table(result_scan(last_query_id())) where upper(\"name\") = upper(" ~ name_lit ~ ")") -%}
    {%- if info.rows | length > 0 -%}
      {{ return({'found': true, 'kind': 'table', 'replaced': info.rows[0][2], 'retention': info.rows[0][1] | int}) }}
    {%- endif -%}
  {%- endif -%}
  {%- set views = run_query("show views like " ~ name_lit ~ in_schema) -%}
  {%- for row in views.rows -%}
    {%- if (row[1] | upper) == (relation.identifier | upper) -%}
      {{ return({'found': true, 'kind': 'view', 'replaced': none, 'retention': none}) }}
    {%- endif -%}
  {%- endfor -%}
  {{ return({'found': false, 'kind': none, 'replaced': none, 'retention': none}) }}
{% endmacro %}

{% macro fingerprint_ledger_ddl(node_rel, seg_rel) %}
  {%- do run_query("
    create table if not exists " ~ node_rel ~ " (
      deploy_id         varchar,
      node_id           varchar,
      node_name         varchar,
      relation_name     varchar,
      parents           array,
      snapshot_at       timestamp_tz,
      loaded_at_column  varchar,
      verdict           varchar,
      detail            varchar,
      registered_at     timestamp_tz,
      finished_at       timestamp_tz,
      replaced          boolean,
      pre_rows          number,
      post_rows         number,
      watermark         timestamp_tz,
      appended_rows     number,
      touched_rows      number,
      touched_old_rows  number,
      buckets_hashed    number,
      buckets_changed   number,
      pre_shape         varchar,
      post_shape        varchar
    )") -%}
  {# Additive upgrades for ledgers created by an earlier revision. #}
  {%- do run_query("alter table " ~ node_rel ~ " add column if not exists pre_shape varchar") -%}
  {%- do run_query("alter table " ~ node_rel ~ " add column if not exists post_shape varchar") -%}
  {%- do run_query("
    create table if not exists " ~ seg_rel ~ " (
      deploy_id   varchar,
      node_id     varchar,
      bucket      date,
      pre_rows    number,
      post_rows   number,
      pre_hash    number,
      post_hash   number,
      changed     boolean,
      hashed_at   timestamp_tz
    )") -%}
{% endmacro %}
