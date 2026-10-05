{#
  Content fingerprint: build guard.

  fingerprint_should_skip() answers "may this node's build be skipped?" from the ledger: true only
  when `fingerprint_skip_unchanged_upstream` is on, at least one parent is registered in this
  deploy, every registered parent finished `unchanged`, `appended` or `skipped`, and the node's
  own source checksum matches the one recorded at its last fingerprinted build of this relation.
  A parent that is `pending`, `new`, `modified`, `unhashable` or `error` blocks. Ephemeral parents
  are looked through to their own parents. A node with no parents, none registered in this
  deploy, or no fingerprinted build of this relation on record is never skipped: the guard cannot
  tell why it is being built. Nor is a node whose own SQL changed, however its parents came out;
  the parents say nothing about that.

  The checksum is the node's own source file, so a change that alters its output only through a
  macro it calls, a var, or YAML-only config leaves the checksum equal and the guard cannot see
  it. Deploy such a change with `fingerprint_skip_unchanged_upstream` off.

  A parent's verdict is its latest real verdict in the deploy; a `skipped` row stands only when
  the parent recorded no verdict in that deploy (see fingerprint_latest_row).

  That verdict is measured from the deploy's snapshot, so it says nothing about a parent written
  between the node's last read and the snapshot, or rebuilt after the node earlier in the same
  deploy. The ledger since the node's last fingerprinted build or skip covers what it recorded;
  three records there build the node: a parent verdict whose snapshot predates that build (the
  parent was rebuilt after the node read it); a blocking verdict or a `pending` row for a parent in
  another deploy (a change the node never read); and a registration of the node under other SQL
  that recorded no verdict (a build whose hooks failed after the table was written).

  Writes the ledger never saw, from a run with the fingerprint off, are invisible to the guard. A
  run that builds parents and their children together leaves them in step. After one that rebuilt
  a parent without its children, or built the node under other SQL, deploy with
  `fingerprint_skip_unchanged_upstream` off.

  fingerprint_mark_skipped() appends the skip so the node's own children can read it.

  These are plain macros for a materialization to call; nothing in this package calls them.
#}

{% macro fingerprint_should_skip(node=none) %}
  {%- if not execute or not dbt_scd2_utils.fingerprint_enabled() or not dbt_scd2_utils.fingerprint_skip_enabled() -%}
    {{ return(false) }}
  {%- endif -%}
  {%- set node = node if node is not none else model -%}
  {%- set parents = [] -%}
  {%- for p in dbt_scd2_utils.fingerprint_parent_ids(node) -%}
    {%- do parents.append(dbt_scd2_utils.fingerprint_lit(p)) -%}
  {%- endfor -%}
  {%- if parents | length == 0 -%}
    {{ return(false) }}
  {%- endif -%}

  {%- set node_rel = dbt_scd2_utils.fingerprint_relation('deploy_node') -%}
  {%- set deploy_lit = dbt_scd2_utils.fingerprint_lit(dbt_scd2_utils.fingerprint_deploy_id()) -%}
  {%- set blocking_in = "('" ~ (dbt_scd2_utils.fingerprint_blocking_verdicts() | map('string') | join("', '")) ~ "')" -%}
  {%- set res = run_query(
      "with latest as (select node_id, verdict from " ~ node_rel
      ~ " where deploy_id = " ~ deploy_lit ~ " and node_id in (" ~ (parents | join(', ')) ~ ")"
      ~ dbt_scd2_utils.fingerprint_latest_row() ~ ")"
      ~ " select count_if(verdict in " ~ blocking_in ~ ") as blocking, count(*) as registered from latest").rows[0] -%}
  {%- set blocking_count = res[0] | int -%}
  {%- set registered = res[1] | int -%}
  {%- if registered == 0 or blocking_count > 0 -%}
    {%- do log("fingerprint: " ~ node.name ~ " parents registered=" ~ registered ~ " blocking=" ~ blocking_count ~ "; building", info=true) -%}
    {{ return(false) }}
  {%- endif -%}

  {#
    The node's own SQL: its checksum has to match the one recorded at its last fingerprinted build of this
    relation. Scoped to the relation because a baseline says "this SQL last built this table"; a table the
    fingerprint has never seen (a model newly guarded, or a new alias) has to be built once before it can skip.
  #}
  {%- set current = dbt_scd2_utils.fingerprint_node_checksum(node) -%}
  {%- set node_lit = dbt_scd2_utils.fingerprint_lit(node.unique_id) -%}
  {%- set rel_lit = dbt_scd2_utils.fingerprint_lit(dbt_scd2_utils.fingerprint_relation_name(node)) -%}
  {%- set base = run_query(
      "select checksum, deploy_id, to_char(finished_at, 'YYYY-MM-DD HH24:MI:SS.FF9 TZHTZM'),"
      ~ " to_char(registered_at, 'YYYY-MM-DD HH24:MI:SS.FF9 TZHTZM') from " ~ node_rel
      ~ " where node_id = " ~ node_lit ~ " and relation_name = " ~ rel_lit
      ~ " and verdict <> 'pending'"
      ~ " order by finished_at desc nulls last, registered_at desc limit 1") -%}
  {%- if base.rows | length == 0 -%}
    {%- do log("fingerprint: " ~ node.name ~ " has no fingerprinted build of this relation on record; building", info=true) -%}
    {{ return(false) }}
  {%- endif -%}
  {%- if current != base.rows[0][0] -%}
    {%- do log("fingerprint: " ~ node.name ~ " checksum differs from its last build in deploy " ~ base.rows[0][1] ~ "; building", info=true) -%}
    {{ return(false) }}
  {%- endif -%}

  {#
    The ledger since the node's last build or skip (`since`). unseen: a later registration of this relation under
    other SQL with no verdict. moved, per parent and deploy on the row that counts: a verdict whose snapshot
    predates `since` (the parent was rebuilt after the node read it), or a blocking verdict or pending row in
    another deploy. This deploy's own verdicts after its snapshot are the ones checked above.
  #}
  {%- set since = dbt_scd2_utils.fingerprint_ts_lit(base.rows[0][2]) -%}
  {%- set since_registered = dbt_scd2_utils.fingerprint_ts_lit(base.rows[0][3]) -%}
  {%- set later = run_query(
      "select (select count(*) from " ~ node_rel
      ~ " where node_id = " ~ node_lit ~ " and relation_name = " ~ rel_lit
      ~ " and registered_at > " ~ since_registered
      ~ " and checksum is distinct from " ~ dbt_scd2_utils.fingerprint_lit(current) ~ ") as unseen,"
      ~ " (select count(*) from (select deploy_id, verdict, snapshot_at from " ~ node_rel
      ~ " where node_id in (" ~ (parents | join(', ')) ~ ") and coalesce(finished_at, registered_at) > " ~ since
      ~ dbt_scd2_utils.fingerprint_latest_row() ~ ")"
      ~ " where (deploy_id <> " ~ deploy_lit ~ " and verdict in " ~ blocking_in ~ ")"
      ~ " or (verdict not in ('pending', 'skipped') and snapshot_at < " ~ since ~ ")) as moved").rows[0] -%}
  {%- if later[0] | int > 0 -%}
    {%- do log("fingerprint: " ~ node.name ~ " was registered under other SQL since its last build and recorded no verdict; building", info=true) -%}
    {{ return(false) }}
  {%- elif later[1] | int > 0 -%}
    {%- do log("fingerprint: " ~ node.name ~ " has " ~ (later[1] | int) ~ " parent verdict(s) since its last build that this deploy's verdicts do not cover; building", info=true) -%}
    {{ return(false) }}
  {%- endif -%}

  {%- if current is none -%}
    {%- do log("fingerprint: " ~ node.name ~ " has no checksum on either side; skipping on parent verdicts alone", info=true) -%}
  {%- else -%}
    {%- do log("fingerprint: " ~ node.name ~ " parents registered=" ~ registered ~ " blocking=0, checksum unchanged; may skip", info=true) -%}
  {%- endif -%}
  {{ return(true) }}
{% endmacro %}

{% macro fingerprint_mark_skipped(node=none, detail=none) %}
  {%- if not execute or not dbt_scd2_utils.fingerprint_enabled() -%}
    {{ return(none) }}
  {%- endif -%}
  {%- set node = node if node is not none else model -%}
  {%- set node_rel = dbt_scd2_utils.fingerprint_relation('deploy_node') -%}
  {%- set key = " where deploy_id = " ~ dbt_scd2_utils.fingerprint_lit(dbt_scd2_utils.fingerprint_deploy_id())
      ~ " and node_id = " ~ dbt_scd2_utils.fingerprint_lit(node.unique_id) -%}
  {%- do run_query(
      "insert into " ~ node_rel
      ~ " (deploy_id, node_id, node_name, relation_name, parents, snapshot_at, loaded_at_column, verdict, detail, registered_at, finished_at, pre_shape, checksum)"
      ~ " select deploy_id, node_id, node_name, relation_name, parents, snapshot_at, loaded_at_column,"
      ~ " 'skipped', " ~ dbt_scd2_utils.fingerprint_lit(detail) ~ ", registered_at, current_timestamp(), pre_shape, checksum"
      ~ " from " ~ node_rel ~ key ~ " and verdict = 'pending'") -%}
{% endmacro %}
