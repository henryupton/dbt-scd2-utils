{#
  Content fingerprint: on-run-start registration.

  Creates the ledger if missing, then writes one `pending` row per selected model, seed or
  snapshot with the deploy id, its parents, its current column shape and `snapshot_at`,
  Snowflake's clock at run start. The post-hook (fingerprint_post) reads the pre-build table
  through Time Travel at that anchor, so it is deliberately the database clock and not dbt's
  run_started_at. A node already registered for this deploy id (an earlier step of the same
  deploy) is left alone.

    on-run-start:
      - "{{ dbt_scd2_utils.fingerprint_register() }}"

  Returns a no-op statement so the hook always has SQL to run.
#}

{% macro fingerprint_register() %}
  {%- if not execute or not dbt_scd2_utils.fingerprint_enabled() -%}
    {{ return('select 1 where false') }}
  {%- endif -%}

  {%- set node_rel = dbt_scd2_utils.fingerprint_relation('deploy_node') -%}
  {%- set seg_rel = dbt_scd2_utils.fingerprint_relation('deploy_segment') -%}
  {%- do dbt_scd2_utils.fingerprint_ledger_ddl(node_rel, seg_rel) -%}

  {%- set deploy_id = dbt_scd2_utils.fingerprint_deploy_id() -%}
  {%- set snapshot_at = run_query("select to_char(current_timestamp()::timestamp_tz, 'YYYY-MM-DD HH24:MI:SS.FF9 TZHTZM')").columns[0].values()[0] -%}

  {%- set selected = [] -%}
  {%- if selected_resources is defined and selected_resources -%}
    {%- for uid in selected_resources -%}{%- do selected.append(uid) -%}{%- endfor -%}
  {%- elif var('fingerprint_select_tag', none) is not none -%}
    {# Fallback for runtimes without selected_resources: every node carrying the tag. #}
    {%- for uid, n in graph.nodes.items() -%}
      {%- if var('fingerprint_select_tag') in (n.tags or []) -%}{%- do selected.append(uid) -%}{%- endif -%}
    {%- endfor -%}
  {%- endif -%}

  {# Collect the nodes, grouped by schema so the shape capture is one query per schema. #}
  {%- set items = [] -%}
  {%- set groups = {} -%}
  {%- for uid in selected -%}
    {%- set n = graph.nodes.get(uid) -%}
    {%- if n is not none and n.resource_type in ['model', 'seed', 'snapshot'] -%}
      {%- set alias = n.alias if (n.alias is defined and n.alias) else n.name -%}
      {%- set group_key = (n.database ~ '.' ~ n.schema) | upper -%}
      {%- if group_key not in groups -%}
        {%- do groups.update({group_key: {'database': n.database, 'schema': n.schema, 'tables': []}}) -%}
      {%- endif -%}
      {%- do groups[group_key]['tables'].append(alias | upper) -%}
      {%- do items.append({'uid': uid, 'node': n, 'alias': alias, 'group_key': group_key}) -%}
    {%- endif -%}
  {%- endfor -%}

  {%- if items | length == 0 -%}
    {%- do log("fingerprint: nothing selected to register for deploy " ~ deploy_id, info=true) -%}
    {{ return('select 1 where false') }}
  {%- endif -%}

  {# One metadata-only SHOW per schema. Accounts that still cap SHOW output return exactly 10,000 rows; that one count falls back to per table. #}
  {%- set shapes = {} -%}
  {%- for group_key, g in groups.items() -%}
    {%- set res = run_query("show columns in schema " ~ g.database ~ "." ~ g.schema) -%}
    {%- if res.rows | length != 10000 -%}
      {%- for t, shape in dbt_scd2_utils.fingerprint_shape_from_show(res).items() -%}
        {%- do shapes.update({group_key ~ '.' ~ t: shape}) -%}
      {%- endfor -%}
    {%- else -%}
      {# A node built for the first time has no relation yet; its pre_shape stays null and the post-hook reads new. #}
      {%- do log("fingerprint: show columns in " ~ g.database ~ "." ~ g.schema ~ " returned exactly 10,000 rows; capturing shapes per table", info=true) -%}
      {%- for t in g.tables | unique -%}
        {%- if adapter.get_relation(database=g.database, schema=g.schema, identifier=t) is not none -%}
          {%- set one = run_query("show columns in table " ~ g.database ~ "." ~ g.schema ~ "." ~ t) -%}
          {%- for tt, shape in dbt_scd2_utils.fingerprint_shape_from_show(one).items() -%}
            {%- do shapes.update({group_key ~ '.' ~ tt: shape}) -%}
          {%- endfor -%}
        {%- endif -%}
      {%- endfor -%}
    {%- endif -%}
  {%- endfor -%}

  {%- set rows = [] -%}
  {%- for item in items -%}
    {%- set n = item.node -%}
    {%- set parents = [] -%}
    {%- for p in n.depends_on.nodes -%}
      {%- if p.startswith('model.') or p.startswith('seed.') or p.startswith('snapshot.') -%}
        {%- do parents.append("'" ~ p ~ "'") -%}
      {%- endif -%}
    {%- endfor -%}
    {%- set relation_name = n.database ~ '.' ~ n.schema ~ '.' ~ item.alias -%}
    {%- do rows.append(
        "select " ~ dbt_scd2_utils.fingerprint_lit(deploy_id) ~ " as deploy_id"
        ~ ", " ~ dbt_scd2_utils.fingerprint_lit(item.uid) ~ " as node_id"
        ~ ", " ~ dbt_scd2_utils.fingerprint_lit(n.name) ~ " as node_name"
        ~ ", " ~ dbt_scd2_utils.fingerprint_lit(relation_name) ~ " as relation_name"
        ~ ", array_construct(" ~ (parents | join(', ')) ~ ") as parents"
        ~ ", " ~ dbt_scd2_utils.fingerprint_ts_lit(snapshot_at) ~ " as snapshot_at"
        ~ ", " ~ dbt_scd2_utils.fingerprint_lit(dbt_scd2_utils.fingerprint_loaded_at_column(n)) ~ " as loaded_at_column"
        ~ ", 'pending' as verdict, null as detail, current_timestamp() as registered_at"
        ~ ", " ~ dbt_scd2_utils.fingerprint_lit(shapes.get(item.group_key ~ '.' ~ (item.alias | upper))) ~ " as pre_shape"
    ) -%}
  {%- endfor -%}

  {%- do run_query(
      "insert into " ~ node_rel
      ~ " (deploy_id, node_id, node_name, relation_name, parents, snapshot_at, loaded_at_column, verdict, detail, registered_at, pre_shape)"
      ~ " with r as (" ~ (rows | join(' union all ')) ~ ")"
      ~ " select r.deploy_id, r.node_id, r.node_name, r.relation_name, r.parents, r.snapshot_at, r.loaded_at_column, r.verdict, r.detail, r.registered_at, r.pre_shape"
      ~ " from r where not exists (select 1 from " ~ node_rel ~ " n where n.deploy_id = r.deploy_id and n.node_id = r.node_id)"
  ) -%}
  {%- do log("fingerprint: registered " ~ (rows | length) ~ " node(s) for deploy " ~ deploy_id ~ " at " ~ snapshot_at, info=true) -%}

  {{ return('select 1 where false') }}
{% endmacro %}
