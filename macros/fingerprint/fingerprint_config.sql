{#
  Content fingerprint: shared settings.

  Vars (all top-level, so they can be set in dbt_project.yml or with --vars):
    fingerprint (bool):                          master switch, default false.
    fingerprint_skip_unchanged_upstream (bool):  lets fingerprint_should_skip() answer true, default false.
    deploy_id (string):                          groups one deploy's ledger rows, default invocation_id.
    fingerprint_schema (string):                 ledger schema, default target.schema.
    fingerprint_loaded_at_column (string):       row arrival column, default _loaded_at.
    fingerprint_exclude (list):                  columns never hashed, default [_batched_at, _written_at, _synthesised_at].
    fingerprint_exclude_scd_columns (bool):      also leave out the scd validity columns (is_current_column and
                                                 valid_to_column), default true. A new version closing an old row then
                                                 reads appended rather than modified. The cost is that a logic change
                                                 which only moves validity reads unchanged; set false where scd2_join
                                                 consumers depend on those columns.

  Per-model meta: fingerprint_loaded_at, fingerprint_exclude (added to the global list).

  The ledger is append-only: registration writes a `pending` row, the post-hook and the guard append the verdict
  row, and the latest row per deploy and node is the one that counts. Nothing updates or deletes, so threads
  finishing together never queue on a table lock.
#}

{# A var as a bool on both engines. dbt-core's macro environment leaves `as_bool` as identity, so "false" would be truthy. #}
{% macro fingerprint_flag(name, default=false) %}
  {{ return((var(name, default) | string | lower) in ['true', '1']) }}
{% endmacro %}

{% macro fingerprint_enabled() %}
  {{ return(dbt_scd2_utils.fingerprint_flag('fingerprint')) }}
{% endmacro %}

{% macro fingerprint_skip_enabled() %}
  {{ return(dbt_scd2_utils.fingerprint_flag('fingerprint_skip_unchanged_upstream')) }}
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
  {%- if dbt_scd2_utils.fingerprint_flag('fingerprint_exclude_scd_columns', true) -%}
    {%- do cols.append(dbt_scd2_utils.get_from_object(pkg, 'is_current_column', default='_is_current')) -%}
    {%- do cols.append(dbt_scd2_utils.get_from_object(pkg, 'valid_to_column', default='_valid_to')) -%}
  {%- endif -%}
  {%- for c in dbt_scd2_utils.fingerprint_node_meta(node).get('fingerprint_exclude', []) or [] -%}{%- do cols.append(c) -%}{%- endfor -%}
  {{ return(cols | map('upper') | list) }}
{% endmacro %}

{% macro fingerprint_blocking_verdicts() %}
  {{ return(['pending', 'new', 'modified', 'unhashable', 'error']) }}
{% endmacro %}

{# The row that counts for a deploy and node: the latest appended. Goes after the where clause. #}
{% macro fingerprint_latest_row() %}
  {{ return(" qualify row_number() over (partition by deploy_id, node_id order by finished_at desc nulls last, registered_at desc) = 1") }}
{% endmacro %}

{#
  The node's source checksum from the manifest, or none when the engine does not expose one. The guard compares
  it with the checksum recorded at the node's last fingerprinted build, so a node whose own SQL changed is never
  skipped on the strength of its parents.
#}
{% macro fingerprint_node_checksum(node) %}
  {%- if node is none -%}
    {{ return(none) }}
  {%- endif -%}
  {%- if node.checksum is defined and node.checksum is not none and node.checksum.checksum is defined -%}
    {{ return(node.checksum.checksum) }}
  {%- endif -%}
  {%- if graph is defined and node.unique_id is defined -%}
    {%- set g = graph.nodes.get(node.unique_id) -%}
    {%- if g is not none and g.checksum is defined and g.checksum is not none and g.checksum.checksum is defined -%}
      {{ return(g.checksum.checksum) }}
    {%- endif -%}
  {%- endif -%}
  {{ return(none) }}
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

{# Column names as stored, from the same SHOW result, so the post-hook can quote them exactly and needs no separate describe. #}
{% macro fingerprint_columns_from_show(res) %}
  {%- set idx = {} -%}
  {%- for name in res.columns | map(attribute='name') -%}{%- do idx.update({name | lower: loop.index0}) -%}{%- endfor -%}
  {%- set cols = [] -%}
  {%- for row in res.rows -%}{%- do cols.append(row[idx['column_name']]) -%}{%- endfor -%}
  {{ return(cols) }}
{% endmacro %}

{# Upper-cased column name -> the SHOW data_type json string, e.g. {"type":"TIMESTAMP_TZ","precision":0,"scale":9,"nullable":true}. #}
{% macro fingerprint_column_types_from_show(res) %}
  {%- set idx = {} -%}
  {%- for name in res.columns | map(attribute='name') -%}{%- do idx.update({name | lower: loop.index0}) -%}{%- endfor -%}
  {%- set types = {} -%}
  {%- for row in res.rows -%}{%- do types.update({row[idx['column_name']] | upper: row[idx['data_type']] | string}) -%}{%- endfor -%}
  {{ return(types) }}
{% endmacro %}

{#
  How to read the watermark back out and re-literalise it in the loaded-at column's own type, so a timestamp_ntz
  column is never compared with a timestamp_tz literal (that shifts by the session offset). None for a type the
  fingerprint cannot bucket by month.
#}
{% macro fingerprint_watermark_format(type_json) %}
  {%- set t = type_json | string | upper -%}
  {%- if '"TYPE":"TIMESTAMP_TZ"' in t -%}
    {{ return({'fmt': 'YYYY-MM-DD HH24:MI:SS.FF9 TZHTZM', 'cast': 'timestamp_tz'}) }}
  {%- elif '"TYPE":"TIMESTAMP_LTZ"' in t -%}
    {{ return({'fmt': 'YYYY-MM-DD HH24:MI:SS.FF9 TZHTZM', 'cast': 'timestamp_ltz'}) }}
  {%- elif '"TYPE":"TIMESTAMP_NTZ"' in t -%}
    {{ return({'fmt': 'YYYY-MM-DD HH24:MI:SS.FF9', 'cast': 'timestamp_ntz'}) }}
  {%- elif '"TYPE":"DATE"' in t -%}
    {{ return({'fmt': 'YYYY-MM-DD', 'cast': 'date'}) }}
  {%- endif -%}
  {{ return(none) }}
{% endmacro %}

{#
  Object state without information_schema.tables (2 s per lookup): `show tables like` is metadata-only, and
  result_scan on it settles created_on against the snapshot in SQL. LIKE treats `_` as a wildcard, so the exact
  name is re-checked. Returns {'found', 'kind', 'replaced', 'retention', 'row_timestamp', 'is_iceberg',
  'is_dynamic'} with kind 'table', 'view' or none. row_timestamp is read here because
  METADATA$ROW_LAST_COMMIT_TIME is an invalid identifier on a table without it, and a hook error fails the node.
#}
{% macro fingerprint_object_info(relation, snap) %}
  {%- set name_lit = dbt_scd2_utils.fingerprint_lit(relation.identifier) -%}
  {%- set in_schema = " in schema " ~ relation.database ~ "." ~ relation.schema -%}
  {%- set tables = run_query("show tables like " ~ name_lit ~ in_schema) -%}
  {%- if tables.rows | length > 0 -%}
    {%- set info = run_query(
        "select \"kind\", \"retention_time\", \"created_on\" > " ~ snap ~ " as replaced,"
        ~ " \"row_timestamp\", \"is_iceberg\", \"is_dynamic\""
        ~ " from table(result_scan(last_query_id())) where upper(\"name\") = upper(" ~ name_lit ~ ")") -%}
    {%- if info.rows | length > 0 -%}
      {%- set r = info.rows[0] -%}
      {{ return({'found': true, 'kind': 'table', 'replaced': r[2], 'retention': r[1] | int,
                 'row_timestamp': (r[3] | string | upper) == 'ON',
                 'is_iceberg': (r[4] | string | upper) == 'Y',
                 'is_dynamic': (r[5] | string | upper) == 'Y'}) }}
    {%- endif -%}
  {%- endif -%}
  {%- set views = run_query("show views like " ~ name_lit ~ in_schema) -%}
  {%- set vidx = {} -%}
  {%- for name in views.columns | map(attribute='name') -%}{%- do vidx.update({name | lower: loop.index0}) -%}{%- endfor -%}
  {%- for row in views.rows -%}
    {%- if (row[vidx['name']] | upper) == (relation.identifier | upper) -%}
      {{ return({'found': true, 'kind': 'view', 'replaced': none, 'retention': none, 'row_timestamp': none, 'is_iceberg': false, 'is_dynamic': false}) }}
    {%- endif -%}
  {%- endfor -%}
  {{ return({'found': false, 'kind': none, 'replaced': none, 'retention': none, 'row_timestamp': none, 'is_iceberg': false, 'is_dynamic': false}) }}
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
      post_shape        varchar,
      checksum          varchar
    )") -%}
  {# Additive upgrades for ledgers created by an earlier revision. #}
  {%- do run_query("alter table " ~ node_rel ~ " add column if not exists pre_shape varchar") -%}
  {%- do run_query("alter table " ~ node_rel ~ " add column if not exists post_shape varchar") -%}
  {%- do run_query("alter table " ~ node_rel ~ " add column if not exists checksum varchar") -%}
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
