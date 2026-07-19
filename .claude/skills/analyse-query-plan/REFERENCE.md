# Reference: reading Snowflake plans for this repo

## Manual procedure (no scripts)

1. Run the model so its MERGE artifact exists:
   `~/.local/bin/dbt build --select <model> --profile default --target dev --vars '{iteration: 2}'`
2. Open `integration_tests/target/run/dbt_scd2_utils_integration_tests/models/.../<model>.sql`.
   The MERGE is `merge into <target> ... using ( <inner> ) AS DBT_INTERNAL_SOURCE ...`.
3. Copy the `<inner>` SELECT. Replace `<model>__dbt_tmp` with the base table name
   (the temp is dropped after the run; the base table is a column superset, fine
   for a structural plan).
4. EXPLAIN it. `dbt show` can't (subquery wrapping), so write a project macro:

   ```jinja
   {% macro _explain_scratch() %}
   {% set q %}{% raw %}
   <paste the inner SELECT here>
   {% endraw %}{% endset %}
   {% if execute %}
   {% set res = run_query('explain using text ' ~ q) %}
   {% for row in res.rows %}{{ log(row[-1], info=True) }}{% endfor %}
   {% endif %}
   {% endmacro %}
   ```

   Save under `integration_tests/macros/` (create the dir; project macros need no
   `dbt deps`), then
   `~/.local/bin/dbt run-operation _explain_scratch --profile default --target dev`.
   Delete the macro afterwards.

`scripts/merge_query.sh` + `scripts/explain.sh` automate steps 2-4.

## Plan operator cheatsheet

`EXPLAIN USING TEXT` prints an indented tree; deeper indent = child (runs first).

- **WithClause X** / **WithReference** — a CTE Snowflake spools. One `WithClause X`
  with multiple `WithReference` nodes = X is computed **once** and reused. This is
  how to prove a CTE referenced several times (e.g. `changes_only` feeding both the
  audit recompute and a `redundant_versions` anti-join) is not re-evaluated. If
  instead the same subtree is duplicated inline, it is re-run.
- **TableScan T {partitionsTotal=P, partitionsAssigned=A}** — `A < P` means micro-
  partition pruning fired; `A == P` is a full scan. Wrapping a join/filter key in a
  function (e.g. `md5(...)` / `generate_surrogate_key`) removes the min/max metadata
  and forces `A == P`; matching raw columns (`equal_null(a, b)`) keeps it prunable.
- **WindowFunction** — a windowed sort. Functions sharing one `PARTITION BY / ORDER BY`
  collapse into a single node/sort; a differing spec (e.g. `... desc`) adds a sort.
  Counting distinct specs tells you how many sorts the recompute costs.
- **AntiJoin** (`NOT IN` / `NOT EXISTS`), **SemiJoin** (`EXISTS` / `IN`) — set-based,
  good. A correlated per-row subquery instead would be the smell.
- **Aggregate groupKeys:[...]** — distinct/group, e.g. the dedupe behind a `NOT IN`.

## Worked example: incremental_scd2 MERGE

Confirmed for `prev_changed_scd2` (Jul 2026): `changes_only` appears as a single
`WithClause CHANGES_ONLY` referenced by both the recompute `WindowFunction` branch
and the `AntiJoin (CHANGES_ONLY._SCD2_KEY = PREVIOUS_RECORD._SCD2_KEY)` — so the
window-heavy compare/collapse chain runs once, not twice. The two `TableScan`s of
the base table showed `partitionsAssigned=1` of `partitionsTotal=2`, i.e. the
`equal_null` key match prunes. Both findings need no code change; they were the
evidence that a proposed "materialise changes_only" rewrite was unnecessary.

Rule of thumb: get the plan before rewriting for efficiency. Twice now the plan
said the optimizer already did the thing a rewrite would have forced.
