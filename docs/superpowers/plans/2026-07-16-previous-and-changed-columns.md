# `_previous` and `_changed` Audit Columns Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add two independent, opt-in SCD-type-2 audit columns: `_previous` (an OBJECT of the prior version's tracked columns) and `_changed` (an OBJECT of per-tracked-column change booleans).

**Architecture:** Both columns are a `lag()` over the same versioned timeline (`partition by unique_key order by updated_at`) that already produces the SCD2 audit columns. Two small expression-builder macros generate the SQL. Planning is centralised in `scd_plan.sql`, which resolves the config, guards the feature to SCD type 2, appends the column names to `audit_columns` (and, under `update_all_previous_records=true`, to `merge_update_cols`), and threads the settings into the type-2 initial-load and incremental SQL builders, which emit the expressions.

**Tech Stack:** dbt (Jinja + SQL macros), Snowflake (OBJECT type, `object_construct_keep_null`, `lag`, `is distinct from`), dbt-utils. Tested via the `integration_tests` dbt project.

## Global Constraints

- Snowflake only; dbt `>=1.0.0`; dbt-utils auto-installed.
- Package options are read from the model `meta` block first, then global `vars` under `dbt_scd2_utils` (existing `get_config_value` / `get_from_object` pattern). Native keys (`materialized`, `unique_key`, `scd_type`) stay top-level.
- Both features default OFF: `track_previous_version=false`, `track_changed_columns=false`. Column-name defaults: `previous_version_column='_PREVIOUS'`, `changed_columns_column='_CHANGED'`.
- **SCD type 2 only.** If either switch is true on an `scd_type` 0 or 1 model, emit a warning at plan time (`exceptions.warn`) and do not produce the columns; the model still builds. The columns are appended only in the type-2 section, so types 0/1 naturally omit them.
- Object keys are lowercased. Known limitation: Snowflake object path access is case-sensitive, so consumers read `_previous:email` / `_changed:email` in lowercase.
- The two switches are enabled per table (per-model `meta`, or folder-level `+meta`), never via a global `vars` switch default.
- `_previous`/`_changed` are NULL for a key's first version. `_changed` uses `IS DISTINCT FROM` over the `varchar` cast of each value (matching the version-detection hash so a created version always has a true flag; null-to-value counts as changed, null-to-null does not). Built with `object_construct_keep_null`.
- Correctness across backfill/out-of-order/collapse is only guaranteed when `update_all_previous_records=true` (same documented caveat as `_change_type`).
- File layout (refactored SCD framework on main): planning in `macros/materializations/scd/scd_plan.sql`; type-2 SQL builders in `macros/materializations/scd/types/type_2/`; column-expression macros in `macros/materializations/scd/columns/`.
- Integration tests require a live Snowflake connection: profile `default`, target `dev` (externalbrowser SSO, may prompt for browser login). Run from `integration_tests/`. Test models use `materialized='incremental_scd2'`. The global `vars` set `created_at_column: _created_at`, so every SCD2 test model must produce a `_created_at` column (a `scd_plan` guard errors otherwise).
- Drafted docs must not contain em dashes or en dashes.

---

### Task 1: Implement `_previous` and `_changed` end to end

One task: enabling a switch is only safe once both the initial-load and incremental paths emit the column, so a partial implementation would break incremental runs for any model with the switch on.

**Files:**
- Create: `macros/materializations/scd/columns/get_previous_version_sql.sql`
- Create: `macros/materializations/scd/columns/get_changed_columns_sql.sql`
- Modify: `macros/materializations/scd/scd_plan.sql`
- Modify: `macros/materializations/scd/types/type_2/get_initial_load_scd2_sql.sql`
- Modify: `macros/materializations/scd/types/type_2/get_incremental_scd2_sql.sql`
- Create (test model): `integration_tests/models/scd2_materialization/prev_changed_scd2.sql`
- Create (test schema): `integration_tests/models/scd2_materialization/prev_changed_schema.yml`
- Create (SCD2-only warning check): `integration_tests/models/scd_materialization/prev_changed_scd1.sql`
- Create (seeds): `integration_tests/seeds/scd2_materialization/prev_changed_raw_1.csv`, `integration_tests/seeds/scd2_materialization/prev_changed_raw_2.csv`, `integration_tests/seeds/scd2_materialization/prev_changed_seeds.yml`
- Create (singular tests): `integration_tests/tests/scd2_materialization/prev_changed_first_version_nulls.sql`, `integration_tests/tests/scd2_materialization/prev_changed_previous_matches_prior.sql`, `integration_tests/tests/scd2_materialization/prev_changed_flags_match_diff.sql`

**Interfaces:**
- Produces: `get_previous_version_sql(scd_check_columns, unique_keys_csv, updated_at_col)` returns a SQL expression (`lag(object_construct_keep_null(...))`), no trailing alias.
- Produces: `get_changed_columns_sql(scd_check_columns, unique_keys_csv, updated_at_col)` returns a SQL `CASE` expression, no trailing alias.
- Produces (arg_dict keys consumed by the type-2 SQL builders): `track_previous_version` (bool), `previous_version_column` (string), `track_changed_columns` (bool), `changed_columns_column` (string).
- Consumes: existing helpers `get_config_value`, `get_from_object`, `get_quoted_csv`; existing `scd_plan` locals `audit_columns`, `merge_update_cols`, `default_arg_dict`, `scd_type`, `scd_check_columns`; existing arg_dict keys `scd_check_columns`, `updated_at_column`.

---

- [ ] **Step 1: Create the integration test model, schema, and seeds (test assets first)**

Create `integration_tests/models/scd2_materialization/prev_changed_scd2.sql`:

```sql
{{
    config(
        materialized='incremental_scd2',
        unique_key=['customer_id'],
        meta={
            'track_previous_version': true,
            'track_changed_columns': true,
            'change_columns': {
                'exclude': ['_written_at', '_created_at']
            }
        }
    )
}}

{#
    Exercises the optional _previous / _changed audit columns.

    Iteration 1 (full refresh): customer 1 has two versions (initial-load lag),
    customer 2 has one (first-version nulls).
    Iteration 2 (incremental): a backfilled row for customer 1 lands BETWEEN its
    two existing versions with genuinely different tracked columns, so the later
    existing version's _previous / _changed must be recomputed in place. This
    relies on update_all_previous_records=true (set globally for this project).
#}

{%- set iteration = var('iteration', 1) | int -%}
{%- set seed_iteration = iteration if iteration <= 2 else 2 -%}

select
    customer_id,
    customer_name,
    email,
    status,
    _created_at::timestamp_tz as _created_at,
    _updated_at::timestamp_tz as _updated_at,
    sysdate() as _written_at
from {{ ref('prev_changed_raw_' ~ seed_iteration) }}
```

Create `integration_tests/seeds/scd2_materialization/prev_changed_raw_1.csv`:

```csv
customer_id,customer_name,email,status,_created_at,_updated_at
1,Ann,ann@old.com,ACTIVE,2024-01-01 10:00:00+0000,2024-01-01 10:00:00+0000
1,Ann,ann@new.com,ACTIVE,2024-01-02 10:00:00+0000,2024-01-02 10:00:00+0000
2,Bob,bob@x.com,ACTIVE,2024-01-01 10:00:00+0000,2024-01-01 10:00:00+0000
```

Create `integration_tests/seeds/scd2_materialization/prev_changed_raw_2.csv`:

```csv
customer_id,customer_name,email,status,_created_at,_updated_at
1,Ann,ann@mid.com,INACTIVE,2024-01-01 15:00:00+0000,2024-01-01 15:00:00+0000
```

Create `integration_tests/seeds/scd2_materialization/prev_changed_seeds.yml`:

```yaml
version: 2

seeds:
  - name: prev_changed_raw_1
    columns:
      - name: customer_id
        data_type: integer
      - name: customer_name
        data_type: varchar
      - name: email
        data_type: varchar
      - name: status
        data_type: varchar
      - name: _created_at
        data_type: timestamp_tz
      - name: _updated_at
        data_type: timestamp_tz

  - name: prev_changed_raw_2
    columns:
      - name: customer_id
        data_type: integer
      - name: customer_name
        data_type: varchar
      - name: email
        data_type: varchar
      - name: status
        data_type: varchar
      - name: _created_at
        data_type: timestamp_tz
      - name: _updated_at
        data_type: timestamp_tz
```

Create `integration_tests/models/scd2_materialization/prev_changed_schema.yml`:

```yaml
version: 2

models:
  - name: prev_changed_scd2
    description: >
      Exercises the optional _previous (prior tracked-column values) and _changed
      (per-tracked-column change booleans) audit columns, including recomputation
      of an existing version's objects after an out-of-order backfill.
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

      - dbt_scd2_utils.continuous_validity_windows:
          arguments:
            key_columns: [customer_id]
            valid_from_column: _valid_from
            valid_to_column: _valid_to

      - dbt_scd2_utils.latest_row_is_current:
          arguments:
            key_columns: [customer_id]
            valid_from_column: _valid_from
            current_column: _is_current
```

Create `integration_tests/models/scd_materialization/prev_changed_scd1.sql` (verifies the SCD2-only warning path: a type-1 model with the switches on must build, warn, and NOT gain the columns):

```sql
{{
    config(
        materialized='scd',
        scd_type=1,
        unique_key=['customer_id'],
        meta={
            'track_previous_version': true,
            'track_changed_columns': true,
            'change_columns': {
                'exclude': ['_written_at', '_created_at']
            }
        }
    )
}}

{#
    track_previous_version / track_changed_columns are SCD2-only. On this type-1
    model they must emit a warning and be ignored (no _PREVIOUS / _CHANGED columns),
    while the model still builds normally. Reuses the prev_changed_raw_1 seed.
#}

select
    customer_id,
    customer_name,
    email,
    status,
    _created_at::timestamp_tz as _created_at,
    _updated_at::timestamp_tz as _updated_at,
    sysdate() as _written_at
from {{ ref('prev_changed_raw_1') }}
```

- [ ] **Step 2: Create the three singular tests**

Create `integration_tests/tests/scd2_materialization/prev_changed_first_version_nulls.sql`:

```sql
-- Fails if a key's first version has non-null objects, or any later version has null objects.
with base as (
    select
        customer_id,
        row_number() over (partition by customer_id order by _updated_at) as rn,
        _previous,
        _changed
    from {{ ref('prev_changed_scd2') }}
)

select *
from base
where (rn = 1 and (_previous is not null or _changed is not null))
   or (rn > 1 and (_previous is null or _changed is null))
```

Create `integration_tests/tests/scd2_materialization/prev_changed_previous_matches_prior.sql`:

```sql
-- Fails if _previous does not match the actual prior version's tracked columns.
with expected as (
    select
        customer_id,
        _updated_at,
        _previous,
        row_number() over (partition by customer_id order by _updated_at) as rn,
        lag(customer_name) over (partition by customer_id order by _updated_at) as prev_customer_name,
        lag(email)         over (partition by customer_id order by _updated_at) as prev_email,
        lag(status)        over (partition by customer_id order by _updated_at) as prev_status
    from {{ ref('prev_changed_scd2') }}
)

select *
from expected
where rn > 1
  and (
       _previous:customer_name::string is distinct from prev_customer_name
    or _previous:email::string         is distinct from prev_email
    or _previous:status::string        is distinct from prev_status
  )
```

Create `integration_tests/tests/scd2_materialization/prev_changed_flags_match_diff.sql`:

```sql
-- Fails if a _changed flag disagrees with the actual diff between the row and its prior version.
with base as (
    select
        customer_id,
        _updated_at,
        _changed,
        row_number() over (partition by customer_id order by _updated_at) as rn,
        (customer_name is distinct from lag(customer_name) over (partition by customer_id order by _updated_at)) as customer_name_changed,
        (email         is distinct from lag(email)         over (partition by customer_id order by _updated_at)) as email_changed,
        (status        is distinct from lag(status)        over (partition by customer_id order by _updated_at)) as status_changed
    from {{ ref('prev_changed_scd2') }}
)

select *
from base
where rn > 1
  and (
       _changed:customer_name::boolean is distinct from customer_name_changed
    or _changed:email::boolean         is distinct from email_changed
    or _changed:status::boolean        is distinct from status_changed
  )
```

- [ ] **Step 3: Run the tests to verify they fail (red)**

```bash
cd /Users/henry.upton/PycharmProjects/dbt-scd2-utils/integration_tests
dbt deps
dbt seed --select prev_changed_raw_1 prev_changed_raw_2 --full-refresh --profile default --target dev
dbt build --select prev_changed_scd2+ --full-refresh --vars '{iteration: 1}' --profile default --target dev
```

Expected: FAIL. The switches are not yet handled, so `prev_changed_scd2` builds without `_previous`/`_changed`, and the singular tests error with an invalid-identifier error on `_previous` / `_changed`.

- [ ] **Step 4: Create `get_previous_version_sql.sql`**

Create `macros/materializations/scd/columns/get_previous_version_sql.sql`:

```jinja
{#
  Builds the expression for the optional `_previous` audit column: an OBJECT holding the
  tracked change columns of the immediately preceding version of the entity.

  Uses lag() over the key's timeline, so the first version of a key yields NULL. Keys are
  lowercased; object_construct_keep_null keeps null-valued keys so a genuinely-null prior
  value is still represented.

  Args:
    scd_check_columns (list): Tracked change columns (the hashed set).
    unique_keys_csv (string): Comma-separated business key columns for partitioning.
    updated_at_col (string): Column used to order the timeline.

  Returns:
    A SQL expression (lag of an object_construct_keep_null); no trailing alias.
#}

{%- macro get_previous_version_sql(scd_check_columns, unique_keys_csv, updated_at_col) -%}
lag(object_construct_keep_null(
  {%- for col in scd_check_columns %}
  '{{ col | lower }}', {{ col }}{{ "," if not loop.last }}
  {%- endfor %}
)) over (partition by {{ unique_keys_csv }} order by {{ updated_at_col }})
{%- endmacro -%}
```

- [ ] **Step 5: Create `get_changed_columns_sql.sql`**

Create `macros/materializations/scd/columns/get_changed_columns_sql.sql`:

```jinja
{#
  Builds the expression for the optional `_changed` audit column: an OBJECT with one
  lowercased key per tracked change column, true when that column differs from the prior
  version (via IS DISTINCT FROM), false otherwise.

  The whole object is NULL for a key's first version (no prior to compare), matching the
  `_previous` column.

  Args:
    scd_check_columns (list): Tracked change columns (the hashed set).
    unique_keys_csv (string): Comma-separated business key columns for partitioning.
    updated_at_col (string): Column used to order the timeline.

  Returns:
    A SQL CASE expression; no trailing alias.
#}

{%- macro get_changed_columns_sql(scd_check_columns, unique_keys_csv, updated_at_col) -%}
case
  when lag({{ updated_at_col }}) over (partition by {{ unique_keys_csv }} order by {{ updated_at_col }}) is null
    then cast(null as object)
  else object_construct_keep_null(
    {%- for col in scd_check_columns %}
    '{{ col | lower }}', (cast({{ col }} as varchar) is distinct from lag(cast({{ col }} as varchar)) over (partition by {{ unique_keys_csv }} order by {{ updated_at_col }})){{ "," if not loop.last }}
    {%- endfor %}
  )
end
{%- endmacro -%}
```

- [ ] **Step 6: Resolve the config and add the SCD2-only warning in `scd_plan.sql`**

In `macros/materializations/scd/scd_plan.sql`, find the `deleted_at_col` resolution line:

```jinja
  {%- set deleted_at_col = dbt_scd2_utils.get_config_value(config, 'deleted_at_column', default=dbt_scd2_utils.get_from_object(var('dbt_scd2_utils', {}), 'deleted_at_column', default=none)) -%}
```

Insert immediately after it:

```jinja
  {%- set track_previous_version = dbt_scd2_utils.get_config_value(config, 'track_previous_version', default=dbt_scd2_utils.get_from_object(var('dbt_scd2_utils', {}), 'track_previous_version', default=false)) -%}
  {%- set previous_version_col = dbt_scd2_utils.get_config_value(config, 'previous_version_column', default=dbt_scd2_utils.get_from_object(var('dbt_scd2_utils', {}), 'previous_version_column', default='_PREVIOUS')) -%}
  {%- set track_changed_columns = dbt_scd2_utils.get_config_value(config, 'track_changed_columns', default=dbt_scd2_utils.get_from_object(var('dbt_scd2_utils', {}), 'track_changed_columns', default=false)) -%}
  {%- set changed_columns_col = dbt_scd2_utils.get_config_value(config, 'changed_columns_column', default=dbt_scd2_utils.get_from_object(var('dbt_scd2_utils', {}), 'changed_columns_column', default='_CHANGED')) -%}
```

Then find the existing SCD-type-0/1 deletion guard block and its closing `{%- endif -%}`:

```jinja
  {%- if scd_type in [0, 1] and deleted_at_col is not none -%}
    {%- set error_message -%}
      deleted_at_column ('{{ deleted_at_col }}') is set on an SCD type {{ scd_type }} model, but
      deletion tracking is not supported for SCD types 0 and 1. Either remove deleted_at_column or
      use scd_type=2.
    {%- endset -%}
    {{ exceptions.raise_compiler_error(error_message) }}
  {%- endif -%}
```

Insert immediately after that block's `{%- endif -%}`:

```jinja
  {# _previous / _changed track version history, which only exists for SCD type 2. On #}
  {# types 0/1 we warn and omit the columns rather than error, so a folder-wide +meta #}
  {# switch does not break a layer that includes a type 0/1 model. The columns are #}
  {# appended only in the type-2 section below, so they are naturally not produced here. #}
  {%- if scd_type in [0, 1] and (track_previous_version or track_changed_columns) -%}
    {%- set warning_message -%}
      track_previous_version / track_changed_columns are set on an SCD type {{ scd_type }} model
      ({{ target_relation }}), but these columns are only produced for SCD type 2 (version history).
      They will be ignored for this model.
    {%- endset -%}
    {{ exceptions.warn(warning_message) }}
  {%- endif -%}
```

- [ ] **Step 7: Append the columns to `merge_update_cols` and `audit_columns` (type-2 section)**

In the same file, find the type-2 `merge_update_cols` block:

```jinja
  {%- set merge_update_cols = [is_current_col, valid_to_col] -%}
  {# Recomputing the change column for every record ensures accuracy. #}
  {# No updating all previous records results in multiple 'I' records. #}
  {%- if update_all_previous_records -%}
    {%- do merge_update_cols.append(change_type_col) -%}
  {%- endif -%}
```

Replace it with:

```jinja
  {%- set merge_update_cols = [is_current_col, valid_to_col] -%}
  {# Recomputing the change column for every record ensures accuracy. #}
  {# No updating all previous records results in multiple 'I' records. #}
  {%- if update_all_previous_records -%}
    {%- do merge_update_cols.append(change_type_col) -%}
    {%- if track_previous_version -%}{%- do merge_update_cols.append(previous_version_col) -%}{%- endif -%}
    {%- if track_changed_columns -%}{%- do merge_update_cols.append(changed_columns_col) -%}{%- endif -%}
  {%- endif -%}

  {# Optional SCD2-only audit columns: prior-version object and per-column change map. #}
  {%- if track_previous_version -%}{%- do audit_columns.append(previous_version_col) -%}{%- endif -%}
  {%- if track_changed_columns -%}{%- do audit_columns.append(changed_columns_col) -%}{%- endif -%}
```

- [ ] **Step 8: Thread the settings into `default_arg_dict` (type-2 section)**

In the same file, find the end of the type-2 `default_arg_dict` literal:

```jinja
      'created_at_column': created_at_col,
      'deleted_at_column': deleted_at_col
  }  %}
```

Replace it with:

```jinja
      'created_at_column': created_at_col,
      'deleted_at_column': deleted_at_col,
      'track_previous_version': track_previous_version,
      'previous_version_column': previous_version_col,
      'track_changed_columns': track_changed_columns,
      'changed_columns_column': changed_columns_col
  }  %}
```

- [ ] **Step 9: Emit the columns in the initial-load SQL**

In `macros/materializations/scd/types/type_2/get_initial_load_scd2_sql.sql`, find the `deleted_at_col` read:

```jinja
    {%- set deleted_at_col = arg_dict.get('deleted_at_column') -%}
```

Insert immediately after it:

```jinja
    {%- set track_previous_version = arg_dict.get('track_previous_version', false) -%}
    {%- set previous_version_col = arg_dict.get('previous_version_column') -%}
    {%- set track_changed_columns = arg_dict.get('track_changed_columns', false) -%}
    {%- set changed_columns_col = arg_dict.get('changed_columns_column') -%}
```

Then find the final select's change-type line and the `from changes_only` that follows it:

```jinja
  {{ dbt_scd2_utils.get_change_type_sql(unique_keys_csv, updated_at_col, deleted_at_col) }} as {{ change_type_col }}
from changes_only
```

Replace with:

```jinja
  {{ dbt_scd2_utils.get_change_type_sql(unique_keys_csv, updated_at_col, deleted_at_col) }} as {{ change_type_col }}
  {%- if track_previous_version %},
  {{ dbt_scd2_utils.get_previous_version_sql(scd_check_columns, unique_keys_csv, updated_at_col) }} as {{ previous_version_col }}
  {%- endif %}
  {%- if track_changed_columns %},
  {{ dbt_scd2_utils.get_changed_columns_sql(scd_check_columns, unique_keys_csv, updated_at_col) }} as {{ changed_columns_col }}
  {%- endif %}
from changes_only
```

- [ ] **Step 10: Emit the columns in the incremental SQL**

In `macros/materializations/scd/types/type_2/get_incremental_scd2_sql.sql`, find the `collapse_redundant_versions` read:

```jinja
    {%- set collapse_redundant_versions = arg_dict.get('collapse_redundant_versions', true) -%}
```

Insert immediately after it:

```jinja
    {%- set track_previous_version = arg_dict.get('track_previous_version', false) -%}
    {%- set previous_version_col = arg_dict.get('previous_version_column') -%}
    {%- set track_changed_columns = arg_dict.get('track_changed_columns', false) -%}
    {%- set changed_columns_col = arg_dict.get('changed_columns_column') -%}
```

Then, in the `scd2_versions` CTE, find:

```jinja
                {{ dbt_scd2_utils.get_change_type_sql(unique_keys_csv, updated_at_col, deleted_at_col) }} as {{ change_type_col }},
                'upsert' as _scd2_op,
                _scd2_key
```

Replace with:

```jinja
                {{ dbt_scd2_utils.get_change_type_sql(unique_keys_csv, updated_at_col, deleted_at_col) }} as {{ change_type_col }},
                {%- if track_previous_version %}
                {{ dbt_scd2_utils.get_previous_version_sql(scd_check_columns, unique_keys_csv, updated_at_col) }} as {{ previous_version_col }},
                {%- endif %}
                {%- if track_changed_columns %}
                {{ dbt_scd2_utils.get_changed_columns_sql(scd_check_columns, unique_keys_csv, updated_at_col) }} as {{ changed_columns_col }},
                {%- endif %}
                'upsert' as _scd2_op,
                _scd2_key
```

Then, in the `redundant_versions` CTE, find:

```jinja
                cast(null as varchar) as {{ change_type_col }},
                'delete' as _scd2_op,
                _scd2_key
```

Replace with:

```jinja
                cast(null as varchar) as {{ change_type_col }},
                {%- if track_previous_version %}
                cast(null as object) as {{ previous_version_col }},
                {%- endif %}
                {%- if track_changed_columns %}
                cast(null as object) as {{ changed_columns_col }},
                {%- endif %}
                'delete' as _scd2_op,
                _scd2_key
```

- [ ] **Step 11: Run the initial-load path to verify green (full refresh)**

```bash
cd /Users/henry.upton/PycharmProjects/dbt-scd2-utils/integration_tests
dbt deps
dbt build --select prev_changed_scd2+ --full-refresh --vars '{iteration: 1}' --profile default --target dev
```

Expected: PASS. The model builds with `_PREVIOUS` and `_CHANGED` OBJECT columns, and all generic tests plus the three singular tests pass on the iteration-1 state (customer 1's second version carries the prior values and the `email` change flag; first versions of both keys have null objects).

- [ ] **Step 12: Run the incremental path to verify green (backfill recompute)**

```bash
cd /Users/henry.upton/PycharmProjects/dbt-scd2-utils/integration_tests
dbt build --select prev_changed_scd2+ --full-refresh --vars '{iteration: 1}' --profile default --target dev
dbt build --select prev_changed_scd2+ --vars '{iteration: 2}' --profile default --target dev
```

Expected: PASS. After the iteration-2 incremental backfill, customer 1 has three versions; the backfilled row and the recomputed later version both carry correct `_previous`/`_changed`, so `prev_changed_previous_matches_prior` and `prev_changed_flags_match_diff` pass (they would fail if the existing version's objects were not recomputed via `merge_update_cols`).

- [ ] **Step 13: Confirm the default-off path and the SCD2-only warning**

```bash
cd /Users/henry.upton/PycharmProjects/dbt-scd2-utils/integration_tests
dbt build --select customers_scd2 customers_scd2_contract customers_scd0 customers_scd1 --full-refresh --vars '{iteration: 1}' --profile default --target dev
dbt build --select prev_changed_scd1 --full-refresh --profile default --target dev 2>&1 | tee /tmp/prev_changed_scd1.log
grep -i "only produced for SCD type 2" /tmp/prev_changed_scd1.log
```

Expected: The first build PASSES with no `_PREVIOUS`/`_CHANGED` columns (none enable the switches) and the enforced contract on `customers_scd2_contract` still matches. The second build PASSES too, and the `grep` finds the SCD2-only warning, confirming `prev_changed_scd1` built without error while the switches were ignored (a type-1 table has no `_PREVIOUS`/`_CHANGED` columns because they are appended only in the type-2 path).

- [ ] **Step 14: Commit**

```bash
cd /Users/henry.upton/PycharmProjects/dbt-scd2-utils
git add macros/materializations/scd/columns/get_previous_version_sql.sql \
        macros/materializations/scd/columns/get_changed_columns_sql.sql \
        macros/materializations/scd/scd_plan.sql \
        macros/materializations/scd/types/type_2/get_initial_load_scd2_sql.sql \
        macros/materializations/scd/types/type_2/get_incremental_scd2_sql.sql \
        integration_tests/models/scd2_materialization/prev_changed_scd2.sql \
        integration_tests/models/scd2_materialization/prev_changed_schema.yml \
        integration_tests/models/scd_materialization/prev_changed_scd1.sql \
        integration_tests/seeds/scd2_materialization/prev_changed_raw_1.csv \
        integration_tests/seeds/scd2_materialization/prev_changed_raw_2.csv \
        integration_tests/seeds/scd2_materialization/prev_changed_seeds.yml \
        integration_tests/tests/scd2_materialization/prev_changed_first_version_nulls.sql \
        integration_tests/tests/scd2_materialization/prev_changed_previous_matches_prior.sql \
        integration_tests/tests/scd2_materialization/prev_changed_flags_match_diff.sql
git commit -m "feat: add optional _previous and _changed SCD2 audit columns"
```

---

### Task 2: Document `_previous` and `_changed` in the README

**Files:**
- Modify: `README.md`

**Interfaces:**
- Consumes: the config option names and defaults from Task 1 (`track_previous_version`, `previous_version_column`, `track_changed_columns`, `changed_columns_column`).

- [ ] **Step 1: Add the four options to the Core Options table**

In `README.md`, in the "Core Options" table, find the `deleted_at_column` row:

```markdown
| `deleted_at_column` | ❌ | none | Column for logical deletion tracking |
```

Insert these rows immediately after it:

```markdown
| `track_previous_version` | ❌ | `false` | (SCD2 only) add an OBJECT column with the prior version's tracked columns |
| `previous_version_column` | ❌ | `_PREVIOUS` | Name of the previous-version object column |
| `track_changed_columns` | ❌ | `false` | (SCD2 only) add an OBJECT column of per-tracked-column change booleans |
| `changed_columns_column` | ❌ | `_CHANGED` | Name of the change-map object column |
```

- [ ] **Step 2: Add a "Previous Version and Change Tracking" section**

In `README.md`, insert this section immediately before the `## Deletion Support` heading:

````markdown
## Previous Version and Change Tracking

Two optional OBJECT columns record, for each version, what the entity looked like before
and which tracked columns moved. Both are off by default, are enabled per model, and apply
to SCD type 2 only (setting either on a type 0 or type 1 model logs a warning and the
columns are simply not produced). You can turn them on for a staging layer without
affecting dimension tables.

```sql
{{
  config(
    materialized='incremental_scd2',
    unique_key=['customer_id'],
    meta={
      'track_previous_version': true,
      'track_changed_columns': true
    }
  )
}}

select
    customer_id,
    email,
    status,
    updated_at as _updated_at
from {{ source('raw', 'customers') }}
```

- **`_previous`** holds the tracked columns of the immediately preceding version. The first
  version of a key has no predecessor, so its `_previous` is `NULL`.
- **`_changed`** holds one boolean per tracked column, `true` when that column changed since
  the prior version. It is `NULL` for a key's first version.

Both objects cover only the tracked change columns (the same set that triggers a new
version), and both use lowercased keys.

| customer_id | email | status | _previous | _changed |
|-------------|-------|--------|-----------|----------|
| 123 | john@old.com | active | null | null |
| 123 | john@new.com | active | `{"email":"john@old.com","status":"active"}` | `{"email":true,"status":false}` |

Enable a whole layer via `dbt_project.yml`:

```yaml
models:
  my_project:
    staging:
      +meta:
        track_previous_version: true
        track_changed_columns: true
    marts:
      # dimension tables leave the switches off
```

**Limitation (case sensitivity):** object keys are stored lowercase, and Snowflake object
path access is case-sensitive, so read them in lowercase (`_previous:email`,
`_changed:email`) even though the underlying columns are uppercase.

**Backfill note:** correct recomputation of these objects for existing versions after an
out-of-order (backfill) arrival requires `update_all_previous_records=true` (the default).
This is the same caveat that applies to `_change_type`.
````

- [ ] **Step 3: Commit**

```bash
cd /Users/henry.upton/PycharmProjects/dbt-scd2-utils
git add README.md
git commit -m "docs: document _previous and _changed audit columns"
```

---

## Self-Review

**1. Spec coverage:**
- Two opt-in columns, default off: Task 1 Steps 6-8. Covered.
- SCD2-only, warn + omit: Step 6 (warning, appends only in type-2 section) + Step 13 (type-1 model with switches on builds, warns, no columns). Covered.
- `_previous` contents (tracked columns of prior version, keep-null, null on first): Step 4 macro + Steps 9-10. Covered.
- `_changed` contents (per-column booleans, IS DISTINCT FROM, null on first): Step 5 macro + Steps 9-10. Covered.
- Lowercase keys + case-sensitivity limitation: Steps 4/5 (`| lower`) and Task 2 Step 2. Covered.
- Two flat independent columns: audit_columns/arg_dict handle each independently (Steps 7-8). Covered.
- Initial-load and incremental paths, plus redundant_versions placeholder: Steps 9-10. Covered.
- Backfill correctness via merge_update_cols under update_all_previous_records: Step 7, tested Step 12. Covered.
- Per-layer enablement: documented Task 2 Step 2; default-off path tested Task 1 Step 13. Covered.
- Testing (first-version null, subsequent values, backfill): Steps 1-2 (assets), 11-12 (runs). Covered.
- Docs: Task 2. Covered.

**2. Placeholder scan:** No TBD/TODO; every code and test block is complete; every command has expected output.

**3. Type consistency:** Macro names `get_previous_version_sql` / `get_changed_columns_sql` and their `(scd_check_columns, unique_keys_csv, updated_at_col)` signatures are identical at definition (Steps 4-5) and every call site (Steps 9-10). The arg_dict keys `track_previous_version` / `previous_version_column` / `track_changed_columns` / `changed_columns_column` are written in Step 8 and read verbatim in Steps 9-10. Local names `track_previous_version` / `previous_version_col` / `track_changed_columns` / `changed_columns_col` are consistent across Steps 6-10. Object columns are `object`-typed everywhere (`object_construct_keep_null`, `cast(null as object)`). Both `scd2_versions` and `redundant_versions` receive the new columns in the same position (after `_change_type`, before `_scd2_op`), keeping the `union all` aligned.
