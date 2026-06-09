# Born-deleted SCD2 records Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A key whose first and only source row arrives already soft-deleted is recorded as a deletion (`_change_type = 'D'`) that is valid from the deletion timestamp (`_valid_from = deleted_at`) and current (`_is_current = true`).

**Architecture:** dbt package (`dbt_scd2_utils`). The SCD2 audit columns are produced by reusable column macros under `macros/materializations/scd/columns/`, called from the type-2 load macros under `macros/materializations/scd/types/type_2/`. The bug is that `get_valid_from_sql` has no `deleted_at` handling, so a born-deleted first record takes the `created_at`/`updated_at` value instead of `deleted_at`. Fix: teach `get_valid_from_sql` that a first record's `valid_from` is `coalesce(deleted_at, created_at, updated_at)`, and pass `deleted_at_col` from both call sites. Reproduced and regression-guarded via a dedicated integration model with `created_at`/`deleted_at`/`updated_at` deliberately distinct. Separately, the public test `first_record_insert` is replaced by `first_record_not_update`, because a born-deleted first record is legitimately a delete, not an insert.

**Tech Stack:** dbt (Snowflake adapter), Jinja/SQL macros, dbt generic + singular tests. Integration tests run from `integration_tests/` with `--profile default --target dev`.

**Working directory:** This plan executes in the worktree at `/Users/henry.upton/PycharmProjects/dbt-scd2-utils/.claude/worktrees/fix+scd2-born-deleted-records` (branch `worktree-fix+scd2-born-deleted-records`, based on `origin/main`). All paths below are relative to the repo root. Run all `dbt` commands from `integration_tests/`.

**Note on `_change_type`:** On this base `get_change_type_sql` already returns `'D'` first when `deleted_at IS NOT NULL`, and `deleted_at_column` is plumbed through `scd_plan`. So `_change_type` is expected to already be `'D'` for the born-deleted record — the singular test asserts it as a guard. The genuinely failing assertion is `_valid_from`. If the reproduction in Task 1 shows `_change_type = 'I'` instead, stop and diagnose `get_change_type_sql` / config resolution before continuing (that would mean `deleted_at_col` is not resolving), rather than assuming the `valid_from` fix is sufficient.

---

### Task 1: Reproduce the born-deleted bug (RED)

**Files:**
- Create: `integration_tests/seeds/scd2_materialization/born_deleted_raw_1.csv`
- Create: `integration_tests/seeds/scd2_materialization/born_deleted_raw_2.csv`
- Modify: `integration_tests/seeds/scd2_materialization/schema.yml`
- Create: `integration_tests/models/scd2_materialization/born_deleted_scd2.sql`
- Modify: `integration_tests/models/scd2_materialization/schema.yml`
- Create: `integration_tests/tests/scd2_materialization/assert_born_deleted_record.sql`

- [ ] **Step 1: Create seed `born_deleted_raw_1.csv` (iteration 1 — initial load)**

Key 100 arrives already soft-deleted. `_created_at`, `deleted_at`, `_updated_at` are deliberately all distinct so the test proves `valid_from` resolves to `deleted_at` (not `created_at`, not `updated_at`).

```csv
customer_id,customer_name,email,status,deleted_at,_created_at,_updated_at
100,Gone Already,gone@example.com,INACTIVE,2024-02-01 00:00:00+0000,2024-01-01 00:00:00+0000,2024-03-01 00:00:00+0000
```

- [ ] **Step 2: Create seed `born_deleted_raw_2.csv` (iteration 2 — incremental)**

Key 100 is unchanged (identical row, so it produces no new version); a second born-deleted key 101 arrives for the first time while the table already exists, exercising the incremental MERGE insert path.

```csv
customer_id,customer_name,email,status,deleted_at,_created_at,_updated_at
100,Gone Already,gone@example.com,INACTIVE,2024-02-01 00:00:00+0000,2024-01-01 00:00:00+0000,2024-03-01 00:00:00+0000
101,Born Deleted Two,bd2@example.com,INACTIVE,2024-05-01 00:00:00+0000,2024-04-01 00:00:00+0000,2024-06-01 00:00:00+0000
```

- [ ] **Step 3: Register the seeds in `integration_tests/seeds/scd2_materialization/schema.yml`**

Append to the end of the existing `seeds:` list (after `customers_raw_5`):

```yaml
  - name: born_deleted_raw_1
    columns:
      - name: customer_id
        data_type: integer
      - name: customer_name
        data_type: varchar
      - name: email
        data_type: varchar
      - name: status
        data_type: varchar
      - name: deleted_at
        data_type: timestamp_tz
      - name: _created_at
        data_type: timestamp_tz
      - name: _updated_at
        data_type: timestamp_tz

  - name: born_deleted_raw_2
    columns:
      - name: customer_id
        data_type: integer
      - name: customer_name
        data_type: varchar
      - name: email
        data_type: varchar
      - name: status
        data_type: varchar
      - name: deleted_at
        data_type: timestamp_tz
      - name: _created_at
        data_type: timestamp_tz
      - name: _updated_at
        data_type: timestamp_tz
```

- [ ] **Step 4: Create the model `integration_tests/models/scd2_materialization/born_deleted_scd2.sql`**

```sql
{{
    config(
        materialized='incremental_scd2',
        unique_key=['customer_id'],
        meta={
            'deleted_at_column': 'deleted_at'
        }
    )
}}

{#
    Born-deleted edge case: a key whose first and only version arrives already
    soft-deleted (ingestion started after the delete happened). It must be
    recorded as a deletion ('D'), valid from deleted_at, and current.

    created_at_column resolves to _created_at from dbt_project.yml vars, so the
    created_at override path in get_valid_from_sql is exercised. _created_at,
    deleted_at and _updated_at are deliberately distinct in the seeds.

    Iteration 1 (full refresh -> initial load): key 100 arrives already deleted.
    Iteration 2 (incremental): a second born-deleted key 101 arrives while the
    table already exists, exercising the MERGE insert path. Key 100 is unchanged.
#}

{%- set iteration = var('iteration', 1) | int -%}
{%- set seed_iteration = 1 if iteration < 2 else 2 -%}

select
    customer_id,
    customer_name,
    email,
    status,
    deleted_at::timestamp_tz as deleted_at,
    _created_at::timestamp_tz as _created_at,
    _updated_at::timestamp_tz as _updated_at
from {{ ref('born_deleted_raw_' ~ seed_iteration) }}
```

- [ ] **Step 5: Wire generic tests for `born_deleted_scd2` in `integration_tests/models/scd2_materialization/schema.yml`**

Append a new model entry under `models:` (after the `customers_scd2_contract` block). Do NOT wire `first_record_insert` here — its replacement is added in Task 3.

```yaml
  - name: born_deleted_scd2
    description: >
      Regression reproduction for the born-deleted edge case: a key whose first
      and only version arrives already soft-deleted. The record must be 'D',
      valid from deleted_at, and current. no_records_after_deletion is the
      primary guard (it asserts valid_from = deleted_at for deleted records).
    tests:
      - dbt_scd2_utils.one_current_per_key:
          arguments:
            key_columns: [customer_id]
            current_column: _is_current

      - dbt_scd2_utils.no_validity_overlaps:
          arguments:
            key_columns: [customer_id]
            valid_from_column: _valid_from
            valid_to_column: _valid_to

      - dbt_scd2_utils.latest_row_is_current:
          arguments:
            key_columns: [customer_id]
            valid_from_column: _valid_from
            current_column: _is_current

      - dbt_scd2_utils.no_records_after_deletion:
          arguments:
            key_columns: [customer_id]
            deleted_at_column: deleted_at
            valid_from_column: _valid_from
            valid_to_column: _valid_to
```

- [ ] **Step 6: Create the singular test `integration_tests/tests/scd2_materialization/assert_born_deleted_record.sql`**

Returns offending rows (test fails) unless every born-deleted key is `'D'`, valid from `deleted_at`, and current.

```sql
-- Born-deleted edge case: a key whose only-ever row arrives already soft-deleted
-- must be recorded as a deletion, valid from the deletion timestamp, and current.
select
    customer_id,
    _change_type,
    _valid_from,
    deleted_at,
    _is_current
from {{ ref('born_deleted_scd2') }}
where customer_id in (100, 101)
  and not (
        _change_type = 'D'
    and _valid_from = deleted_at
    and _is_current = true
  )
```

- [ ] **Step 7: Seed, then build iteration 1 and confirm it FAILS (RED)**

Run from `integration_tests/`:

```bash
dbt seed --profile default --target dev --select born_deleted_raw_1 born_deleted_raw_2
dbt build --profile default --target dev --select born_deleted_scd2+ --full-refresh --vars '{iteration: 1}'
```

Expected: the model builds, but tests FAIL — specifically `no_records_after_deletion_born_deleted_scd2_...` and `assert_born_deleted_record` (because `_valid_from` resolves to `2024-01-01` (created_at) rather than `2024-02-01` (deleted_at)). Confirm `_change_type` is `'D'` in the failure output of `assert_born_deleted_record`; if it shows `'I'`, stop and diagnose config/`get_change_type_sql` per the note above.

- [ ] **Step 8: Commit the failing reproduction**

```bash
git add integration_tests/seeds/scd2_materialization/born_deleted_raw_1.csv \
        integration_tests/seeds/scd2_materialization/born_deleted_raw_2.csv \
        integration_tests/seeds/scd2_materialization/schema.yml \
        integration_tests/models/scd2_materialization/born_deleted_scd2.sql \
        integration_tests/models/scd2_materialization/schema.yml \
        integration_tests/tests/scd2_materialization/assert_born_deleted_record.sql
git commit -m "test: reproduce born-deleted SCD2 edge case (RED)" \
           -m "Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 2: Fix `valid_from` for born-deleted records (GREEN)

**Files:**
- Modify: `macros/materializations/scd/columns/get_valid_from_sql.sql`
- Modify: `macros/materializations/scd/types/type_2/get_initial_load_scd2_sql.sql:93`
- Modify: `macros/materializations/scd/types/type_2/get_incremental_scd2_sql.sql:162`

- [ ] **Step 1: Rewrite `macros/materializations/scd/columns/get_valid_from_sql.sql`**

Replace the whole macro body with the version below. It adds a `deleted_at_col` parameter and makes the first record's `valid_from` equal `coalesce(deleted_at, created_at, updated_at)`. Non-first records are unchanged (`updated_at`), preserving validity-window continuity (the prior version's `valid_to` is `lead(updated_at)`).

```sql
{#
  Generates SQL to determine the valid_from timestamp for SCD Type 2 records.

  For the FIRST version of a key (no previous record) valid_from is
  coalesce(deleted_at, created_at, updated_at):
    - a born-deleted record (deleted_at set) is valid from the deletion time;
    - otherwise created_at when configured;
    - otherwise updated_at.
  Non-first versions always use updated_at, so each window starts where the
  previous version's valid_to (lead(updated_at)) ends.

  Args:
    unique_keys_csv (string): Comma-separated unique key columns for partitioning.
    updated_at_col (string): Column used for chronological ordering.
    created_at_col (string, optional): Creation timestamp; first version of a key
      uses it when there is no deleted_at.
    deleted_at_col (string, optional): Logical deletion timestamp; a born-deleted
      first version uses it.

  Returns:
    SQL expression returning the valid_from timestamp for each record.
#}

{%- macro get_valid_from_sql(unique_keys_csv, updated_at_col, created_at_col=None, deleted_at_col=None) -%}
  {%- set first_record_parts = [] -%}
  {%- if deleted_at_col is not none -%}
    {%- do first_record_parts.append(deleted_at_col ~ '::timestamp_tz') -%}
  {%- endif -%}
  {%- if created_at_col is not none -%}
    {%- do first_record_parts.append(created_at_col ~ '::timestamp_tz') -%}
  {%- endif -%}
  {%- do first_record_parts.append(updated_at_col ~ '::timestamp_tz') -%}

  {%- if first_record_parts | length > 1 -%}
    case
      when lag({{ updated_at_col }}) over (partition by {{ unique_keys_csv }} order by {{ updated_at_col }}) is null
        then coalesce({{ first_record_parts | join(', ') }})
      else {{ updated_at_col }}::timestamp_tz
    end
  {%- else -%}
    {{ updated_at_col }}::timestamp_tz
  {%- endif -%}
{%- endmacro %}
```

- [ ] **Step 2: Pass `deleted_at_col` from the initial-load call site**

In `macros/materializations/scd/types/type_2/get_initial_load_scd2_sql.sql`, the `valid_from` line currently reads:

```sql
  {{ dbt_scd2_utils.get_valid_from_sql(unique_keys_csv, updated_at_col, created_at_col) }} as {{ valid_from_col }},
```

Change it to (add `deleted_at_col`, which is already set earlier in this macro):

```sql
  {{ dbt_scd2_utils.get_valid_from_sql(unique_keys_csv, updated_at_col, created_at_col, deleted_at_col) }} as {{ valid_from_col }},
```

- [ ] **Step 3: Pass `deleted_at_col` from the incremental call site**

In `macros/materializations/scd/types/type_2/get_incremental_scd2_sql.sql`, the `valid_from` line currently reads:

```sql
        {{ dbt_scd2_utils.get_valid_from_sql(unique_keys_csv, updated_at_col, created_at_col) }} as {{ valid_from_col }},
```

Change it to:

```sql
        {{ dbt_scd2_utils.get_valid_from_sql(unique_keys_csv, updated_at_col, created_at_col, deleted_at_col) }} as {{ valid_from_col }},
```

- [ ] **Step 4: Rebuild iteration 1 and confirm GREEN (initial-load path)**

```bash
dbt build --profile default --target dev --select born_deleted_scd2+ --full-refresh --vars '{iteration: 1}'
```

Expected: PASS. `no_records_after_deletion` and `assert_born_deleted_record` now pass because `_valid_from` for key 100 is `2024-02-01` (deleted_at).

- [ ] **Step 5: Run iteration 2 and confirm GREEN (incremental path)**

```bash
dbt build --profile default --target dev --select born_deleted_scd2+ --vars '{iteration: 2}'
```

Expected: PASS. Key 101 is inserted via the MERGE as `'D'`, `_valid_from = 2024-05-01` (its deleted_at), current; key 100 is unchanged.

- [ ] **Step 6: Commit the fix**

```bash
git add macros/materializations/scd/columns/get_valid_from_sql.sql \
        macros/materializations/scd/types/type_2/get_initial_load_scd2_sql.sql \
        macros/materializations/scd/types/type_2/get_incremental_scd2_sql.sql
git commit -m "fix: born-deleted records use deleted_at as valid_from (GREEN)" \
           -m "Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 3: Replace `first_record_insert` with `first_record_not_update`

**Files:**
- Delete: `tests/generic/test_first_record_insert.sql`
- Create: `tests/generic/test_first_record_not_update.sql`
- Modify: `integration_tests/models/scd2_materialization/schema.yml` (customers wiring + born_deleted wiring)
- Modify: `README.md:356`

- [ ] **Step 1: Create `tests/generic/test_first_record_not_update.sql`**

```sql
{% test first_record_not_update(model, key_columns, valid_from_column, change_type_column) %}
{#
    A key's first record (earliest valid_from) must not be an Update ('U').
    The first record is either an Insert ('I') for a normally-created entity, or
    a Delete ('D') for a born-deleted entity (ingestion started after the soft
    delete). An Update as the first record implies a prior version that does not
    exist, which is invalid.
#}

with first_records as (
    select
        {{ dbt_scd2_utils.get_quoted_csv(key_columns) }},
        {{ change_type_column }},
        row_number() over (partition by {{ dbt_scd2_utils.get_quoted_csv(key_columns) }} order by {{ valid_from_column }}) as rn
    from {{ model }}
),
invalid_first_records as (
    select
        {{ dbt_scd2_utils.get_quoted_csv(key_columns) }},
        {{ change_type_column }}
    from first_records
    where rn = 1
        and {{ change_type_column }} = 'U'
)
select * from invalid_first_records

{% endtest %}
```

- [ ] **Step 2: Delete the old test file**

```bash
git rm tests/generic/test_first_record_insert.sql
```

- [ ] **Step 3: Update the `customers_scd2` wiring in `integration_tests/models/scd2_materialization/schema.yml`**

Replace the existing block (currently lines ~37-41):

```yaml
      - dbt_scd2_utils.first_record_insert:
          arguments:
            key_columns: [customer_id]
            valid_from_column: _valid_from
            change_type_column: _change_type
```

with:

```yaml
      - dbt_scd2_utils.first_record_not_update:
          arguments:
            key_columns: [customer_id]
            valid_from_column: _valid_from
            change_type_column: _change_type
```

- [ ] **Step 4: Wire `first_record_not_update` to `born_deleted_scd2`**

In the same file, inside the `born_deleted_scd2` model's `tests:` list (added in Task 1, Step 5), append:

```yaml
      - dbt_scd2_utils.first_record_not_update:
          arguments:
            key_columns: [customer_id]
            valid_from_column: _valid_from
            change_type_column: _change_type
```

This proves a born-deleted first record (`'D'`) passes the new test — whereas the old `first_record_insert` would have failed it.

- [ ] **Step 5: Update `README.md:356`**

Replace the line:

```markdown
- `first_record_insert`: First records have change_type = 'I'
```

with:

```markdown
- `first_record_not_update`: A key's first record is an insert or delete, never an update
```

- [ ] **Step 6: Run born_deleted and customers suites; confirm GREEN**

```bash
dbt build --profile default --target dev --select born_deleted_scd2+ --vars '{iteration: 2}'
dbt build --profile default --target dev --select customers_scd2 --full-refresh --vars '{iteration: 1}'
```

Expected: both PASS. `first_record_not_update` passes for `born_deleted_scd2` (first record is `'D'`) and for `customers_scd2` (first records are `'I'`).

- [ ] **Step 7: Commit the rename**

```bash
git add tests/generic/test_first_record_not_update.sql \
        integration_tests/models/scd2_materialization/schema.yml \
        README.md
git commit -m "refactor!: replace first_record_insert with first_record_not_update" \
           -m "A born-deleted first record is a valid 'D', so the first-record invariant is 'not an update', not 'is an insert'. Breaking change to the package test surface." \
           -m "Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 4: Full regression run across the customers sequence

**Files:** none (verification only).

- [ ] **Step 1: Run the full customers_scd2 sequence (iterations 1-6)**

This exercises inserts, updates, deletes and resurrections with the changed `get_valid_from_sql`, confirming no regression in normal SCD2 behaviour. Run from `integration_tests/`:

```bash
./test_scd2_sequence.sh 1 6 customers_scd2
```

Expected: summary reports all iterations SUCCESS, 0 failures.

- [ ] **Step 2: Run the born_deleted sequence end to end**

```bash
./test_scd2_sequence.sh 1 2 born_deleted_scd2
```

Expected: all SUCCESS, 0 failures.

- [ ] **Step 3: Final confirmation**

Confirm both runs are green. If anything fails, return to systematic-debugging — do not paper over a failure. No commit needed (verification only).

---

## Notes for the executor

- Always run `dbt` from the `integration_tests/` directory.
- If `dbt seed`/`dbt build` reports a profile error, confirm the `default` profile / `dev` target exists in your dbt profiles. The personal Snowflake trial is dead — use the shared `default` profile.
- `store_failures: true` is set, so failing test rows are written to a table you can inspect for diagnosis.
- Do not bring across the `collapse_redundant_versions` / `ooo_backfill` work — it lives on the meta-config branch and is intentionally out of scope here.
