---
name: analyse-query-plan
description: Interrogate the Snowflake query plan for a dbt model in this repo (especially the incremental_scd2 MERGE) to check CTE reuse, join strategy, partition pruning, and window/sort counts. Use when judging query efficiency, verifying an optimization actually changed the plan, deciding whether a SQL rewrite is worthwhile, or whenever you need to run EXPLAIN against Snowflake through dbt-fusion.
---

# Analyse a query plan

Pull the real Snowflake `EXPLAIN` for a model's generated SQL and read it, so
efficiency decisions rest on the plan rather than a guess. Plan **structure**
(CTE reuse, join type, pruning, window count) is data-size-independent, so the
tiny `integration_tests` data gives valid structural answers.

## Prerequisites

- Binary: `~/.local/bin/dbt` (dbt-fusion). Run from `integration_tests/` with
  `--profile default --target dev`. First call opens Okta SSO in a browser, so
  run in the **main session** (subagents can't complete the browser login).
- Run the target model at least once first, so its compiled MERGE lands in
  `integration_tests/target/run/.../<model>.sql`. `dbt compile` does **not**
  emit a custom materialization's MERGE — only a run does.

## Quick start

```bash
# 1. Materialise the model so its run artifact exists (incremental = iter 2+):
~/.local/bin/dbt build --select prev_changed_scd2 --profile default --target dev --vars '{iteration: 2}'

# 2. Extract the MERGE inner query (temp relation rewritten to the base table):
.claude/skills/analyse-query-plan/scripts/merge_query.sh prev_changed_scd2 > /tmp/q.sql

# 3. EXPLAIN it and print the operator tree + pruning summary:
.claude/skills/analyse-query-plan/scripts/explain.sh /tmp/q.sql
```

For any ad-hoc SQL, skip steps 1-2 and pass a file straight to `explain.sh`.

## Reading the plan

The signals that answer most efficiency questions:

| Question | Look for |
|----------|----------|
| Is a CTE evaluated once or N times? | One `WithClause X` with several `WithReference` nodes = computed **once** and reused. Repeated inline subtrees = re-evaluated. |
| Is the target pruned? | `TableScan ... {partitionsTotal=T, partitionsAssigned=A}`. `A < T` = pruning works; `A == T` = full scan (often a hash/function wrapping the key). |
| How many sorts? | Count distinct `WindowFunction` groups by their `PARTITION BY / ORDER BY`; each distinct spec is a sort. |
| How is a NOT IN / EXISTS run? | `AntiJoin` (NOT IN) / `SemiJoin` (EXISTS), not a correlated scan. |

Full cheatsheet, the manual (no-script) procedure, and worked SCD2 examples:
see [REFERENCE.md](REFERENCE.md).

## Gotchas

- `dbt show --inline "explain ..."` fails: it wraps the query in a subquery and
  `EXPLAIN` can't be a subquery. Use a `run-operation` macro (what `explain.sh`
  generates) that calls `run_query('explain using text ' ~ q)`.
- In dbt-fusion, `result.print_table()` does not surface from `run-operation`.
  Log rows instead: `{% for row in res.rows %}{{ log(row[-1], info=True) }}{% endfor %}`.
- The `<model>__dbt_tmp` temp relation is dropped after the run. `merge_query.sh`
  rewrites it to the base table (a column superset), which is fine for structural
  plans. It does change row counts, so ignore absolute cost, read structure.
- `integration_tests/macros/` may not exist; the scripts create and clean it up.
  Project macros need no `dbt deps`.
