{#
  Content fingerprint: post-hook verdict.

    models:
      +post-hook: "{{ dbt_scd2_utils.fingerprint_post(this) }}"

  Compares the table as it stands now with the table as it stood at `snapshot_at`, read through
  Time Travel, and appends one of these to the ledger:

    new         object created or replaced after the snapshot (create or replace, first build), or a view
    unchanged   nothing written and the row count is unchanged, or every hashed bucket matches
    appended    every row this build wrote sits above the pre-build watermark (max loaded_at)
    modified    a column was added, removed or retyped, or at least one month at or below the
                watermark hashes or counts differently
    unhashable  no loaded_at column on one side, a loaded_at column that is not a timestamp or date,
                or no row timestamps on the table (ROW_TIMESTAMP off, which includes Iceberg)
    error       relation missing, or no Time Travel (retention 0)

  Everything except unchanged, appended and skipped blocks a child's skip.

  METADATA$ROW_LAST_COMMIT_TIME names the rows this build wrote. When they are a subset of the
  table only their loaded_at months are hashed; when every row was rewritten (truncate + insert,
  insert overwrite, unguarded merge) all months at or below the watermark are. Row counts are
  compared for every month either way, so a deletion in an untouched month is still caught. Rows
  with a null loaded_at form their own bucket and count as old rows, never as appended.

  The watermark is read and re-literalised in the loaded_at column's own type, so a timestamp_ntz
  column is never compared with a timestamp_tz literal (which shifts by the session offset).
  Columns are quoted as stored. GEOGRAPHY and GEOMETRY cannot be hashed and are left out; the
  detail names them.

  Cost shape, measured on production tables: object state and shape come from SHOW (metadata,
  about 0.1 s each); the counts are filtered scalar subqueries, which prune on the commit-time
  metadata and the watermark (well under 1 GB scanned on a 4 TB table). A conditional aggregate
  such as count_if cannot prune and scanned the whole table. Only the hash reads data, and only
  for the months the build touched.

  The hook runs inside the node, so a SQL error it does not anticipate fails that node's build.
  The known cases above are recorded as verdicts instead of raised.
#}

{% macro fingerprint_post(relation) %}
  {%- if not execute or not dbt_scd2_utils.fingerprint_enabled() -%}
    {{ return('select 1 where false') }}
  {%- endif -%}

  {%- set node_rel = dbt_scd2_utils.fingerprint_relation('deploy_node') -%}
  {%- set seg_rel = dbt_scd2_utils.fingerprint_relation('deploy_segment') -%}
  {%- set deploy_id = dbt_scd2_utils.fingerprint_deploy_id() -%}
  {%- set node_id = model.unique_id -%}
  {%- set key = " where deploy_id = " ~ dbt_scd2_utils.fingerprint_lit(deploy_id) ~ " and node_id = " ~ dbt_scd2_utils.fingerprint_lit(node_id) -%}

  {%- set reg = run_query("select to_char(snapshot_at, 'YYYY-MM-DD HH24:MI:SS.FF9 TZHTZM'), verdict, loaded_at_column, pre_shape from " ~ node_rel ~ key ~ dbt_scd2_utils.fingerprint_latest_row()) -%}
  {%- if reg.rows | length == 0 -%}
    {%- do log("fingerprint: " ~ relation ~ " is not registered for deploy " ~ deploy_id, info=true) -%}
    {{ return('select 1 where false') }}
  {%- endif -%}
  {%- set snapshot_at = reg.rows[0][0] -%}
  {%- set prior_verdict = reg.rows[0][1] -%}
  {%- set loaded_at_col = reg.rows[0][2] -%}
  {%- set pre_shape = reg.rows[0][3] -%}
  {%- if prior_verdict == 'skipped' -%}
    {{ return('select 1 where false') }}
  {%- endif -%}

  {%- set snap = dbt_scd2_utils.fingerprint_ts_lit(snapshot_at) -%}
  {%- set tt = " at(timestamp => " ~ snap ~ ")" -%}
  {%- set out = {'verdict': 'error', 'detail': none, 'replaced': none, 'pre_rows': none, 'post_rows': none,
                 'watermark_sql': 'null', 'appended_rows': none, 'touched_rows': none, 'touched_old_rows': none,
                 'buckets_hashed': none, 'buckets_changed': none, 'post_shape': none} -%}

  {# 1. Object state. created_on moves only when the object is created or replaced. #}
  {%- set info = dbt_scd2_utils.fingerprint_object_info(relation, snap) -%}

  {%- if not info.found -%}
    {%- do out.update({'verdict': 'error', 'detail': 'relation not found'}) -%}
  {%- elif info.kind != 'table' -%}
    {%- do out.update({'verdict': 'new', 'detail': info.kind ~ ': not fingerprinted'}) -%}
  {%- elif info.replaced -%}
    {%- set n = run_query("select count(*) from " ~ relation).columns[0].values()[0] -%}
    {%- do out.update({'verdict': 'new', 'detail': 'object created after snapshot', 'replaced': true, 'post_rows': n | int}) -%}
  {%- elif info.retention == 0 -%}
    {%- do out.update({'verdict': 'error', 'detail': 'no Time Travel: data_retention_time_in_days is 0', 'replaced': false}) -%}
  {%- elif not info.row_timestamp -%}
    {# METADATA$ROW_LAST_COMMIT_TIME is an invalid identifier without ROW_TIMESTAMP; recorded, not raised. #}
    {%- do out.update({'verdict': 'unhashable', 'replaced': false,
                       'detail': 'iceberg table: row timestamps unsupported' if info.is_iceberg else 'no row timestamps: ROW_TIMESTAMP is off'}) -%}
  {%- else -%}
    {%- do out.update({'replaced': false}) -%}

    {# 2. Shape: a column added, removed or retyped on a surviving object is a change the hash cannot see. #}
    {%- set shape_res = run_query("show columns in table " ~ relation) -%}
    {%- set post_shape = dbt_scd2_utils.fingerprint_shape_from_show(shape_res).get(relation.identifier | upper) -%}
    {%- do out.update({'post_shape': post_shape}) -%}

    {%- if pre_shape is not none and post_shape is not none and pre_shape != post_shape -%}
      {%- set pre_cols = pre_shape.split(',') -%}
      {%- set post_cols = post_shape.split(',') -%}
      {%- set added = [] -%}
      {%- set removed = [] -%}
      {%- for c in post_cols -%}{%- if c not in pre_cols -%}{%- do added.append(c) -%}{%- endif -%}{%- endfor -%}
      {%- for c in pre_cols -%}{%- if c not in post_cols -%}{%- do removed.append(c) -%}{%- endif -%}{%- endfor -%}
      {%- do out.update({'verdict': 'modified', 'detail': 'shape changed; now has ' ~ (added | join(' ') or 'nothing new') ~ '; lost ' ~ (removed | join(' ') or 'nothing')}) -%}
    {%- else -%}

      {# 3. Columns common to both sides, minus exclusions, quoted as stored. #}
      {%- set now_cols = dbt_scd2_utils.fingerprint_columns_from_show(shape_res) -%}
      {%- set col_types = dbt_scd2_utils.fingerprint_column_types_from_show(shape_res) -%}
      {%- set pre_cols = run_query("select * from " ~ relation ~ tt ~ " where 1 = 0").columns | map(attribute='name') | map('upper') | list -%}
      {%- set excluded = dbt_scd2_utils.fingerprint_excluded_columns(model) -%}
      {%- set loaded_upper = loaded_at_col | upper -%}
      {%- set hash_cols = [] -%}
      {%- set not_hashable = [] -%}
      {%- set pick = {'loaded': none} -%}
      {%- for c in now_cols -%}
        {%- set cu = c | upper -%}
        {%- if cu == loaded_upper -%}{%- do pick.update({'loaded': c}) -%}{%- endif -%}
        {%- if cu in pre_cols and cu not in excluded -%}
          {%- set t = col_types.get(cu, '') | upper -%}
          {%- if '"TYPE":"GEOGRAPHY"' in t or '"TYPE":"GEOMETRY"' in t -%}
            {%- do not_hashable.append(c) -%}
          {%- else -%}
            {%- do hash_cols.append(adapter.quote(c)) -%}
          {%- endif -%}
        {%- endif -%}
      {%- endfor -%}
      {%- set wm = dbt_scd2_utils.fingerprint_watermark_format(col_types.get(loaded_upper)) -%}

      {%- if pick.loaded is none or loaded_upper not in pre_cols -%}
        {%- do out.update({'verdict': 'unhashable', 'detail': 'no ' ~ loaded_at_col ~ ' column on both sides'}) -%}
      {%- elif wm is none -%}
        {%- do out.update({'verdict': 'unhashable', 'detail': loaded_at_col ~ ' is not a timestamp or date: ' ~ col_types.get(loaded_upper)}) -%}
      {%- else -%}
        {%- set L = adapter.quote(pick.loaded) -%}
        {%- set bucket = "date_trunc('month', " ~ L ~ ")::date" -%}
        {%- set written = "metadata$row_last_commit_time >= " ~ snap -%}

        {# 4. Counts. The watermark first, as a literal in the column's own type, so every count below is a prunable filter. #}
        {%- set pre = run_query("select count(*), to_char(max(" ~ L ~ "), '" ~ wm.fmt ~ "') from " ~ relation ~ tt).rows[0] -%}
        {%- set pre_rows = pre[0] | int -%}
        {%- set watermark = pre[1] -%}
        {%- set w = none -%}
        {%- if watermark is none -%}
          {%- set old = L ~ " is null" -%}
          {%- set counts_sql = "select (select count(*) from " ~ relation ~ ") as post_n,"
              ~ " (select count(*) from " ~ relation ~ ") as appended,"
              ~ " (select count(*) from " ~ relation ~ " where " ~ written ~ ") as touched,"
              ~ " (select count(*) from " ~ relation ~ " where " ~ written ~ " and " ~ old ~ ") as touched_old" -%}
        {%- else -%}
          {%- set w = "'" ~ watermark ~ "'::" ~ wm.cast -%}
          {%- set old = "(" ~ L ~ " <= " ~ w ~ " or " ~ L ~ " is null)" -%}
          {%- set counts_sql = "select (select count(*) from " ~ relation ~ ") as post_n,"
              ~ " (select count(*) from " ~ relation ~ " where " ~ L ~ " > " ~ w ~ ") as appended,"
              ~ " (select count(*) from " ~ relation ~ " where " ~ written ~ ") as touched,"
              ~ " (select count(*) from " ~ relation ~ " where " ~ written ~ " and " ~ old ~ ") as touched_old" -%}
          {%- do out.update({'watermark_sql': w ~ '::timestamp_tz'}) -%}
        {%- endif -%}
        {%- set r = run_query(counts_sql).rows[0] -%}
        {%- set post_rows = r[0] | int -%}
        {%- set appended = r[1] | int -%}
        {%- set touched = r[2] | int -%}
        {%- set touched_old = r[3] | int -%}
        {%- do out.update({'pre_rows': pre_rows, 'post_rows': post_rows,
                           'appended_rows': appended, 'touched_rows': touched, 'touched_old_rows': touched_old}) -%}

        {%- if touched == 0 and post_rows == pre_rows -%}
          {%- do out.update({'verdict': 'unchanged', 'detail': 'no rows written'}) -%}
        {%- elif touched_old == 0 and (post_rows - pre_rows) == touched -%}
          {%- do out.update({'verdict': 'appended', 'detail': 'every written row is above the watermark'}) -%}
        {%- elif watermark is none -%}
          {%- do out.update({'verdict': 'modified', 'detail': 'no watermark at snapshot and rows were written'}) -%}
        {%- else -%}
          {# 5. Hash the touched months (all months when every row was rewritten); count every month. The null bucket rides along. #}
          {%- set cols_csv = (hash_cols | join(', ')) if hash_cols | length > 0 else '1' -%}
          {%- set in_touched = "(" ~ bucket ~ " in (select bucket from touched where bucket is not null)"
              ~ " or (" ~ bucket ~ " is null and exists (select 1 from touched where bucket is null)))" -%}
          {%- do run_query(
              "insert into " ~ seg_rel ~ " (deploy_id, node_id, bucket, pre_rows, post_rows, pre_hash, post_hash, changed, hashed_at)"
              ~ " with touched as (select distinct " ~ bucket ~ " as bucket from " ~ relation
              ~ "   where " ~ written ~ " and " ~ old ~ "),"
              ~ " pre_counts as (select " ~ bucket ~ " as bucket, count(*) as n from " ~ relation ~ tt ~ " where " ~ old ~ " group by 1),"
              ~ " post_counts as (select " ~ bucket ~ " as bucket, count(*) as n from " ~ relation ~ " where " ~ old ~ " group by 1),"
              ~ " pre_hash as (select " ~ bucket ~ " as bucket, hash_agg(" ~ cols_csv ~ ") as h from " ~ relation ~ tt
              ~ "   where " ~ old ~ " and " ~ in_touched ~ " group by 1),"
              ~ " post_hash as (select " ~ bucket ~ " as bucket, hash_agg(" ~ cols_csv ~ ") as h from " ~ relation
              ~ "   where " ~ old ~ " and " ~ in_touched ~ " group by 1),"
              ~ " buckets as (select bucket from pre_counts union select bucket from post_counts)"
              ~ " select " ~ dbt_scd2_utils.fingerprint_lit(deploy_id) ~ ", " ~ dbt_scd2_utils.fingerprint_lit(node_id) ~ ", b.bucket,"
              ~ " pc.n, qc.n, ph.h, qh.h,"
              ~ " coalesce(pc.n, 0) <> coalesce(qc.n, 0)"
              ~ "   or ((ph.h is null) <> (qh.h is null))"
              ~ "   or (ph.h is not null and qh.h is not null and ph.h <> qh.h) as changed,"
              ~ " current_timestamp()"
              ~ " from buckets b"
              ~ " left join pre_counts pc on equal_null(pc.bucket, b.bucket)"
              ~ " left join post_counts qc on equal_null(qc.bucket, b.bucket)"
              ~ " left join pre_hash ph on equal_null(ph.bucket, b.bucket)"
              ~ " left join post_hash qh on equal_null(qh.bucket, b.bucket)") -%}
          {# Segment rows are appended too; the insert stamps every row with one hashed_at, so the latest stamp is this pass. #}
          {%- set agg = run_query("select count_if(pre_hash is not null or post_hash is not null), count_if(changed) from " ~ seg_rel ~ key
              ~ " and hashed_at = (select max(hashed_at) from " ~ seg_rel ~ key ~ ")").rows[0] -%}
          {%- set hashed = agg[0] | int -%}
          {%- set changed = agg[1] | int -%}
          {%- do out.update({'buckets_hashed': hashed, 'buckets_changed': changed}) -%}
          {%- if changed > 0 -%}
            {%- do out.update({'verdict': 'modified', 'detail': changed ~ ' month bucket(s) differ at or below the watermark'}) -%}
          {%- elif appended > 0 -%}
            {%- do out.update({'verdict': 'appended', 'detail': 'touched months hash equal; ' ~ appended ~ ' row(s) above the watermark'}) -%}
          {%- else -%}
            {%- do out.update({'verdict': 'unchanged', 'detail': 'rows rewritten but every hashed month is equal'}) -%}
          {%- endif -%}
        {%- endif -%}
        {%- if not_hashable | length > 0 and out.verdict in ['unchanged', 'appended'] -%}
          {%- do out.update({'detail': out.detail ~ '; not hashed (geography/geometry): ' ~ (not_hashable | join(' '))}) -%}
        {%- endif -%}
      {%- endif -%}
    {%- endif -%}
  {%- endif -%}

  {# Append the verdict row, copying the registration fields from the pending row. Nothing is updated in place. #}
  {%- do run_query(
      "insert into " ~ node_rel
      ~ " (deploy_id, node_id, node_name, relation_name, parents, snapshot_at, loaded_at_column, verdict, detail, registered_at, finished_at,"
      ~ " replaced, pre_rows, post_rows, watermark, appended_rows, touched_rows, touched_old_rows, buckets_hashed, buckets_changed, pre_shape, post_shape, checksum)"
      ~ " select deploy_id, node_id, node_name, relation_name, parents, snapshot_at, loaded_at_column,"
      ~ " " ~ dbt_scd2_utils.fingerprint_lit(out.verdict)
      ~ ", " ~ dbt_scd2_utils.fingerprint_lit(out.detail)
      ~ ", registered_at, current_timestamp()"
      ~ ", " ~ dbt_scd2_utils.fingerprint_lit(out.replaced)
      ~ ", " ~ dbt_scd2_utils.fingerprint_lit(out.pre_rows)
      ~ ", " ~ dbt_scd2_utils.fingerprint_lit(out.post_rows)
      ~ ", " ~ out.watermark_sql
      ~ ", " ~ dbt_scd2_utils.fingerprint_lit(out.appended_rows)
      ~ ", " ~ dbt_scd2_utils.fingerprint_lit(out.touched_rows)
      ~ ", " ~ dbt_scd2_utils.fingerprint_lit(out.touched_old_rows)
      ~ ", " ~ dbt_scd2_utils.fingerprint_lit(out.buckets_hashed)
      ~ ", " ~ dbt_scd2_utils.fingerprint_lit(out.buckets_changed)
      ~ ", pre_shape"
      ~ ", " ~ dbt_scd2_utils.fingerprint_lit(out.post_shape)
      ~ ", checksum"
      ~ " from " ~ node_rel ~ key ~ " and verdict = 'pending'") -%}
  {%- do log("fingerprint: " ~ relation ~ " => " ~ out.verdict ~ (" (" ~ out.detail ~ ")" if out.detail else ""), info=true) -%}

  {{ return('select 1 where false') }}
{% endmacro %}
