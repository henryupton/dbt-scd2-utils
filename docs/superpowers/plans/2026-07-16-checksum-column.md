# `_checksum` Audit Column Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add an optional, opt-in `_checksum` audit column (md5 content fingerprint of the business columns) to all SCD types (0/1/2), matching the envato-data-platform staging `_checksum`.

**Architecture:** `_checksum = generate_surrogate_key(checksum_columns)`, where `checksum_columns` is the business columns (incl. natural key) minus the audit and lifecycle columns, sorted alphabetically. Computed once in `scd_plan.sql` and threaded into every builder. It is a per-row fingerprint (no window functions), so type 0 sets it once, type 1 recomputes on overwrite, and type 2 sets it per version (immutable, so not in `merge_update_cols`).

**Tech Stack:** dbt (Jinja + SQL macros), Snowflake, dbt-utils (`generate_surrogate_key`). Tested via the `integration_tests` dbt project.

## Global Constraints

- Snowflake only; dbt `>=1.0.0`; dbt-utils auto-installed.
- Options read from model `meta` first, then global `vars` under `dbt_scd2_utils` (`get_config_value` / `get_from_object`). Native keys (`materialized`, `unique_key`, `scd_type`) stay top-level; `scd_type` for the generic `scd` materialization goes in `meta` (repo convention).
- Feature defaults OFF: `track_checksum=false`. Column-name default: `checksum_column='_CHECKSUM'`.
- Applies to ALL SCD types (0, 1, 2). No type warning (unlike `track_previous_version` / `track_changed_columns`).
- `checksum_columns` = all source columns EXCEPT the audit columns and the lifecycle columns (`updated_at_column`, `created_at_column`, `deleted_at_column`), INCLUDING the natural key. Sorted alphabetically (case-insensitive) before hashing so the checksum is independent of select-list order.
- Hash is `dbt_utils.generate_surrogate_key` (md5 hex), identical in algorithm to envato's `generate_checksum`. Intentionally NOT the same value as the internal `_scd2_hash`.
- Per-type write behavior: type 0 insert-once; type 1 insert AND recompute on the matched `update set`; type 2 emit per version on insert, `cast(null as varchar)` placeholder in `redundant_versions`, NOT in `merge_update_cols`.
- Column order in every builder: `_checksum` sits after `_change_type` and before the type-2-only `_previous` / `_changed`.
- File layout: `macros/materializations/scd/` (planning in `scd_plan.sql`; type builders under `types/type_{0,1,2}/`; column macros under `columns/`).
- Integration tests need live Snowflake: profile `default`, target `dev`, dbt-fusion binary `/Users/henry.upton/.local/bin/dbt` (the `dbt` on PATH is a Cloud CLI wrapper). Global `vars` set `created_at_column: _created_at`, so every test model must produce a `_created_at` column.
- Drafted docs must not contain em dashes or en dashes.

---

### Task 1: Implement `_checksum` across all SCD types

One task: enabling `track_checksum` adds the column to every type's insert list, so `scd_plan` and all six builders must emit it together or an un-updated type errors on an insert-list mismatch.

**Files:**
- Create: `macros/materializations/scd/columns/get_checksum_sql.sql`
- Modify: `macros/materializations/scd/scd_plan.sql`
- Modify: `macros/materializations/scd/types/type_2/get_initial_load_scd2_sql.sql`
- Modify: `macros/materializations/scd/types/type_2/get_incremental_scd2_sql.sql`
- Modify: `macros/materializations/scd/types/type_0/get_initial_load_scd0_sql.sql`
- Modify: `macros/materializations/scd/types/type_0/get_incremental_scd0_sql.sql`
- Modify: `macros/materializations/scd/types/type_1/get_initial_load_scd1_sql.sql`
- Modify: `macros/materializations/scd/types/type_1/get_incremental_scd1_sql.sql`
- Create (test models): `integration_tests/models/scd2_materialization/checksum_scd2.sql`, `integration_tests/models/scd_materialization/checksum_scd0.sql`, `integration_tests/models/scd_materialization/checksum_scd1.sql`
- Create (seeds): `integration_tests/seeds/scd2_materialization/checksum_raw_1.csv`, `checksum_raw_2.csv`, `checksum_seeds.yml`
- Create (singular tests): `integration_tests/tests/scd2_materialization/checksum_scd2_matches_business_cols.sql`, `integration_tests/tests/scd_materialization/checksum_scd0_matches_business_cols.sql`, `integration_tests/tests/scd_materialization/checksum_scd1_matches_business_cols.sql`

**Interfaces:**
- Produces: `get_checksum_sql(checksum_columns)` returns a `generate_surrogate_key(...)` expression; no trailing alias.
- Produces (arg_dict keys consumed by every builder): `track_checksum` (bool), `checksum_column` (string), `checksum_columns` (pre-sorted list).
- Consumes: `dbt_utils.generate_surrogate_key`; `get_config_value` / `get_from_object`; `list_difference` / `list_union`; existing `scd_plan` locals `audit_columns`, `dest_column_names_upper`, `updated_at_col` / `created_at_col` / `deleted_at_col`.

---

- [ ] **Step 1: Create the test models**

Create `integration_tests/models/scd2_materialization/checksum_scd2.sql`:

```sql
{{
    config(
        materialized='incremental_scd2',
        unique_key=['customer_id'],
        meta={'track_checksum': true}
    )
}}

{#
    Exercises the optional _checksum column for SCD type 2. Business columns are
    selected in NON-alphabetical order (status, email, customer_name, customer_id)
    on purpose: the checksum must be identical to a fixed alphabetical oracle, which
    only holds if the implementation sorts the columns before hashing.
#}

{%- set iteration = var('iteration', 1) | int -%}
{%- set seed_iteration = iteration if iteration <= 2 else 2 -%}

select
    status,
    email,
    customer_name,
    customer_id,
    _updated_at::timestamp_tz as _created_at,
    _updated_at::timestamp_tz as _updated_at
from {{ ref('checksum_raw_' ~ seed_iteration) }}
```

Create `integration_tests/models/scd_materialization/checksum_scd0.sql` (identical body, type 0):

```sql
{{
    config(
        materialized='scd',
        unique_key=['customer_id'],
        meta={'scd_type': 0, 'track_checksum': true}
    )
}}

{#
    SCD type 0 _checksum: computed once for the retained (earliest) row, never updated.
    Non-alphabetical select order to exercise the checksum sort.
#}

{%- set iteration = var('iteration', 1) | int -%}
{%- set seed_iteration = iteration if iteration <= 2 else 2 -%}

select
    status,
    email,
    customer_name,
    customer_id,
    _updated_at::timestamp_tz as _created_at,
    _updated_at::timestamp_tz as _updated_at
from {{ ref('checksum_raw_' ~ seed_iteration) }}
```

Create `integration_tests/models/scd_materialization/checksum_scd1.sql` (identical body, type 1):

```sql
{{
    config(
        materialized='scd',
        unique_key=['customer_id'],
        meta={'scd_type': 1, 'track_checksum': true}
    )
}}

{#
    SCD type 1 _checksum: recomputed when a key's business columns are overwritten in
    place. Iteration 2 overwrites customer 1 with new content, so its _checksum must
    change to match (not stay stale). Non-alphabetical select order exercises the sort.
#}

{%- set iteration = var('iteration', 1) | int -%}
{%- set seed_iteration = iteration if iteration <= 2 else 2 -%}

select
    status,
    email,
    customer_name,
    customer_id,
    _updated_at::timestamp_tz as _created_at,
    _updated_at::timestamp_tz as _updated_at
from {{ ref('checksum_raw_' ~ seed_iteration) }}
```

- [ ] **Step 2: Create the seeds**

Create `integration_tests/seeds/scd2_materialization/checksum_raw_1.csv`:

```csv
customer_id,customer_name,email,status,_updated_at
1,Ann,ann@old.com,ACTIVE,2024-01-01 10:00:00+0000
1,Ann,ann@new.com,ACTIVE,2024-01-02 10:00:00+0000
2,Bob,bob@x.com,ACTIVE,2024-01-01 10:00:00+0000
```

Create `integration_tests/seeds/scd2_materialization/checksum_raw_2.csv`:

```csv
customer_id,customer_name,email,status,_updated_at
1,Ann,ann@newest.com,INACTIVE,2024-01-03 10:00:00+0000
```

Create `integration_tests/seeds/scd2_materialization/checksum_seeds.yml`:

```yaml
version: 2

seeds:
  - name: checksum_raw_1
    columns:
      - name: customer_id
        data_type: integer
      - name: customer_name
        data_type: varchar
      - name: email
        data_type: varchar
      - name: status
        data_type: varchar
      - name: _updated_at
        data_type: timestamp_tz

  - name: checksum_raw_2
    columns:
      - name: customer_id
        data_type: integer
      - name: customer_name
        data_type: varchar
      - name: email
        data_type: varchar
      - name: status
        data_type: varchar
      - name: _updated_at
        data_type: timestamp_tz
```

- [ ] **Step 3: Create the three singular tests**

Each asserts the same invariant: a row's stored `_checksum` equals the md5 fingerprint of that row's own business columns, listed in the fixed alphabetical order the implementation must produce. Because the models select columns in a different order, this fails unless the implementation sorts; and for type 1 it fails if an overwritten row's checksum is left stale.

Create `integration_tests/tests/scd2_materialization/checksum_scd2_matches_business_cols.sql`:

```sql
select customer_id, _updated_at, _checksum
from {{ ref('checksum_scd2') }}
where _checksum is distinct from {{ dbt_utils.generate_surrogate_key(['customer_id', 'customer_name', 'email', 'status']) }}
```

Create `integration_tests/tests/scd_materialization/checksum_scd0_matches_business_cols.sql`:

```sql
select customer_id, _checksum
from {{ ref('checksum_scd0') }}
where _checksum is distinct from {{ dbt_utils.generate_surrogate_key(['customer_id', 'customer_name', 'email', 'status']) }}
```

Create `integration_tests/tests/scd_materialization/checksum_scd1_matches_business_cols.sql`:

```sql
select customer_id, _checksum
from {{ ref('checksum_scd1') }}
where _checksum is distinct from {{ dbt_utils.generate_surrogate_key(['customer_id', 'customer_name', 'email', 'status']) }}
```

- [ ] **Step 4: Run the tests to verify they fail (red)**

```bash
cd /Users/henry.upton/PycharmProjects/dbt-scd2-utils/integration_tests
DBT=/Users/henry.upton/.local/bin/dbt
$DBT deps
$DBT seed --select checksum_raw_1 checksum_raw_2 --full-refresh --profile default --target dev
$DBT build --select checksum_scd0+ checksum_scd1+ checksum_scd2+ --full-refresh --vars '{iteration: 1}' --profile default --target dev
```

Expected: FAIL. `track_checksum` is not yet handled, so no `_CHECKSUM` column exists and the three singular tests error with an invalid-identifier on `_checksum`.

- [ ] **Step 5: Create `get_checksum_sql.sql`**

Create `macros/materializations/scd/columns/get_checksum_sql.sql`:

```jinja
{#
  Builds the expression for the optional `_checksum` audit column: an md5 content
  fingerprint of the model's business columns, via dbt_utils.generate_surrogate_key (the
  same function envato-data-platform's generate_checksum wraps). The caller passes the
  already-sorted checksum_columns (business columns including the natural key, minus the
  audit and lifecycle columns).

  Args:
    checksum_columns (list): Business columns to fingerprint, pre-sorted.

  Returns:
    A SQL expression (generate_surrogate_key call); no trailing alias.
#}

{%- macro get_checksum_sql(checksum_columns) -%}
{{ dbt_utils.generate_surrogate_key(checksum_columns) }}
{%- endmacro -%}
```

- [ ] **Step 6: Resolve config and compute `checksum_columns` in `scd_plan.sql`**

In `macros/materializations/scd/scd_plan.sql`, find the `changed_columns_col` resolution:

```jinja
  {%- set changed_columns_col = dbt_scd2_utils.get_config_value(config, 'changed_columns_column', default=dbt_scd2_utils.get_from_object(var('dbt_scd2_utils', {}), 'changed_columns_column', default='_CHANGED')) -%}
```

Insert immediately after it:

```jinja
  {%- set track_checksum = dbt_scd2_utils.get_config_value(config, 'track_checksum', default=dbt_scd2_utils.get_from_object(var('dbt_scd2_utils', {}), 'track_checksum', default=false)) -%}
  {%- set checksum_col = dbt_scd2_utils.get_config_value(config, 'checksum_column', default=dbt_scd2_utils.get_from_object(var('dbt_scd2_utils', {}), 'checksum_column', default='_CHECKSUM')) -%}
```

Then find the `should_full_refresh` line:

```jinja
  {%- set should_full_refresh = (should_full_refresh() or existing_relation is none) -%}
```

Insert immediately BEFORE it:

```jinja
  {# Content checksum column set: business columns (including the natural key) minus the #}
  {# audit and lifecycle columns, sorted so the fingerprint is independent of select-list #}
  {# order. Shared by all SCD types; computed from the base audit columns before the #}
  {# type-2-only _previous / _changed are appended. #}
  {%- set checksum_lifecycle_cols = [] -%}
  {%- for c in [updated_at_col, created_at_col, deleted_at_col] -%}
    {%- if c is not none -%}{%- do checksum_lifecycle_cols.append(c) -%}{%- endif -%}
  {%- endfor -%}
  {%- set checksum_columns = dbt_scd2_utils.list_difference(dest_column_names_upper, dbt_scd2_utils.list_union(audit_columns, checksum_lifecycle_cols), case_insensitive=true) | sort -%}
  {%- if track_checksum -%}{%- do audit_columns.append(checksum_col) -%}{%- endif -%}

```

- [ ] **Step 7: Thread the settings into the type-0/1 arg_dict**

In the same file, find the end of the type-0/1 `arg_dict` literal:

```jinja
        'created_at_column': created_at_col
    } -%}
```

Replace it with:

```jinja
        'created_at_column': created_at_col,
        'track_checksum': track_checksum,
        'checksum_column': checksum_col,
        'checksum_columns': checksum_columns
    } -%}
```

- [ ] **Step 8: Thread the settings into the type-2 default_arg_dict**

In the same file, find the end of the type-2 `default_arg_dict` literal:

```jinja
      'changed_columns_column': changed_columns_col
  }  %}
```

Replace it with:

```jinja
      'changed_columns_column': changed_columns_col,
      'track_checksum': track_checksum,
      'checksum_column': checksum_col,
      'checksum_columns': checksum_columns
  }  %}
```

- [ ] **Step 9: Emit `_checksum` in the type-2 initial load**

In `macros/materializations/scd/types/type_2/get_initial_load_scd2_sql.sql`, find the `changed_columns_col` read:

```jinja
    {%- set changed_columns_col = arg_dict.get('changed_columns_column') -%}
```

Insert immediately after it:

```jinja
    {%- set track_checksum = arg_dict.get('track_checksum', false) -%}
    {%- set checksum_col = arg_dict.get('checksum_column') -%}
    {%- set checksum_columns = arg_dict.get('checksum_columns', []) -%}
```

Then find (the change-type line followed by the `_previous` conditional):

```jinja
  {{ dbt_scd2_utils.get_change_type_sql(unique_keys_csv, updated_at_col, deleted_at_col) }} as {{ change_type_col }}
  {%- if track_previous_version %},
```

Replace with:

```jinja
  {{ dbt_scd2_utils.get_change_type_sql(unique_keys_csv, updated_at_col, deleted_at_col) }} as {{ change_type_col }}
  {%- if track_checksum %},
  {{ dbt_scd2_utils.get_checksum_sql(checksum_columns) }} as {{ checksum_col }}
  {%- endif %}
  {%- if track_previous_version %},
```

- [ ] **Step 10: Emit `_checksum` in the type-2 incremental**

In `macros/materializations/scd/types/type_2/get_incremental_scd2_sql.sql`, find the `changed_columns_col` read:

```jinja
    {%- set changed_columns_col = arg_dict.get('changed_columns_column') -%}
```

Insert immediately after it:

```jinja
    {%- set track_checksum = arg_dict.get('track_checksum', false) -%}
    {%- set checksum_col = arg_dict.get('checksum_column') -%}
    {%- set checksum_columns = arg_dict.get('checksum_columns', []) -%}
```

Then, in `scd2_versions`, find:

```jinja
                {{ dbt_scd2_utils.get_change_type_sql(unique_keys_csv, updated_at_col, deleted_at_col) }} as {{ change_type_col }},
                {%- if track_previous_version %}
```

Replace with:

```jinja
                {{ dbt_scd2_utils.get_change_type_sql(unique_keys_csv, updated_at_col, deleted_at_col) }} as {{ change_type_col }},
                {%- if track_checksum %}
                {{ dbt_scd2_utils.get_checksum_sql(checksum_columns) }} as {{ checksum_col }},
                {%- endif %}
                {%- if track_previous_version %}
```

Then, in `redundant_versions`, find:

```jinja
                cast(null as varchar) as {{ change_type_col }},
                {%- if track_previous_version %}
                cast(null as object) as {{ previous_version_col }},
```

Replace with:

```jinja
                cast(null as varchar) as {{ change_type_col }},
                {%- if track_checksum %}
                cast(null as varchar) as {{ checksum_col }},
                {%- endif %}
                {%- if track_previous_version %}
                cast(null as object) as {{ previous_version_col }},
```

- [ ] **Step 11: Emit `_checksum` in the type-0 builders**

In `macros/materializations/scd/types/type_0/get_initial_load_scd0_sql.sql`, find the `change_type_col` read:

```jinja
    {%- set change_type_col = arg_dict['change_type_column'] -%}
```

Insert immediately after it:

```jinja
    {%- set track_checksum = arg_dict.get('track_checksum', false) -%}
    {%- set checksum_col = arg_dict.get('checksum_column') -%}
    {%- set checksum_columns = arg_dict.get('checksum_columns', []) -%}
```

Then find (final select, 4-space indent):

```jinja
    'I' as {{ change_type_col }}
from dedup
```

Replace with:

```jinja
    'I' as {{ change_type_col }}
    {%- if track_checksum %},
    {{ dbt_scd2_utils.get_checksum_sql(checksum_columns) }} as {{ checksum_col }}
    {%- endif %}
from dedup
```

In `macros/materializations/scd/types/type_0/get_incremental_scd0_sql.sql`, find the `change_type_col` read and insert the same three `arg_dict.get` lines after it. Then find (source select, 8-space indent):

```jinja
        'I' as {{ change_type_col }}
    from dedup
```

Replace with:

```jinja
        'I' as {{ change_type_col }}
        {%- if track_checksum %},
        {{ dbt_scd2_utils.get_checksum_sql(checksum_columns) }} as {{ checksum_col }}
        {%- endif %}
    from dedup
```

- [ ] **Step 12: Emit `_checksum` in the type-1 builders (incl. overwrite recompute)**

In `macros/materializations/scd/types/type_1/get_initial_load_scd1_sql.sql`, find the `change_type_col` read and insert the same three `arg_dict.get` lines after it. Then find (final select, 4-space indent):

```jinja
    'I' as {{ change_type_col }}
from dedup
```

Replace with:

```jinja
    'I' as {{ change_type_col }}
    {%- if track_checksum %},
    {{ dbt_scd2_utils.get_checksum_sql(checksum_columns) }} as {{ checksum_col }}
    {%- endif %}
from dedup
```

In `macros/materializations/scd/types/type_1/get_incremental_scd1_sql.sql`, find the `change_type_col` read and insert the same three `arg_dict.get` lines after it. Then find (source select, 8-space indent):

```jinja
        'I' as {{ change_type_col }}
    from dedup
```

Replace with:

```jinja
        'I' as {{ change_type_col }}
        {%- if track_checksum %},
        {{ dbt_scd2_utils.get_checksum_sql(checksum_columns) }} as {{ checksum_col }}
        {%- endif %}
    from dedup
```

Then find the matched `update set` block:

```jinja
when matched then update set
    {% for col in update_cols %}
        DBT_INTERNAL_DEST.{{ col }} = DBT_INTERNAL_SOURCE.{{ col }}{% if not loop.last %},{% endif %}
    {%- endfor %}
```

Replace with (recompute the checksum when the row's business columns are overwritten):

```jinja
when matched then update set
    {% for col in update_cols %}
        DBT_INTERNAL_DEST.{{ col }} = DBT_INTERNAL_SOURCE.{{ col }}{% if not loop.last %},{% endif %}
    {%- endfor %}
    {%- if track_checksum %},
        DBT_INTERNAL_DEST.{{ checksum_col }} = DBT_INTERNAL_SOURCE.{{ checksum_col }}
    {%- endif %}
```

- [ ] **Step 13: Verify green, all three types (full refresh, iteration 1)**

```bash
cd /Users/henry.upton/PycharmProjects/dbt-scd2-utils/integration_tests
DBT=/Users/henry.upton/.local/bin/dbt
$DBT deps
$DBT build --select checksum_scd0+ checksum_scd1+ checksum_scd2+ --full-refresh --vars '{iteration: 1}' --profile default --target dev
```

Expected: PASS. All three models build with a `_CHECKSUM` column and their singular tests pass (the checksum matches the alphabetical oracle, proving the sort works despite the non-alphabetical select order).

- [ ] **Step 14: Verify the type-1 overwrite recompute and type-2 versioning (incremental, iteration 2)**

```bash
cd /Users/henry.upton/PycharmProjects/dbt-scd2-utils/integration_tests
DBT=/Users/henry.upton/.local/bin/dbt
$DBT build --select checksum_scd0+ checksum_scd1+ checksum_scd2+ --vars '{iteration: 2}' --profile default --target dev
```

Expected: PASS. Iteration 2 overwrites customer 1 in `checksum_scd1` with new business values; its `_checksum` is recomputed (the oracle test would fail if it were left stale). `checksum_scd2` adds a new version with its own checksum; `checksum_scd0` retains the original row and checksum.

- [ ] **Step 15: Confirm the default-off path is unaffected**

```bash
cd /Users/henry.upton/PycharmProjects/dbt-scd2-utils/integration_tests
DBT=/Users/henry.upton/.local/bin/dbt
$DBT run --select customers_scd2 customers_scd2_contract customers_scd0 customers_scd1 --full-refresh --vars '{iteration: 1}' --profile default --target dev
```

Expected: PASS. None enable `track_checksum`, so no `_CHECKSUM` column appears and the enforced contract on `customers_scd2_contract` still matches. (Use `dbt run` to sidestep pre-existing `store_failures` audit-table privilege noise in the shared dev schema.)

- [ ] **Step 16: Commit**

```bash
cd /Users/henry.upton/PycharmProjects/dbt-scd2-utils
git checkout -- integration_tests/package-lock.yml 2>/dev/null || true
git add macros/materializations/scd/columns/get_checksum_sql.sql \
        macros/materializations/scd/scd_plan.sql \
        macros/materializations/scd/types/type_2/get_initial_load_scd2_sql.sql \
        macros/materializations/scd/types/type_2/get_incremental_scd2_sql.sql \
        macros/materializations/scd/types/type_0/get_initial_load_scd0_sql.sql \
        macros/materializations/scd/types/type_0/get_incremental_scd0_sql.sql \
        macros/materializations/scd/types/type_1/get_initial_load_scd1_sql.sql \
        macros/materializations/scd/types/type_1/get_incremental_scd1_sql.sql \
        integration_tests/models/scd2_materialization/checksum_scd2.sql \
        integration_tests/models/scd_materialization/checksum_scd0.sql \
        integration_tests/models/scd_materialization/checksum_scd1.sql \
        integration_tests/seeds/scd2_materialization/checksum_raw_1.csv \
        integration_tests/seeds/scd2_materialization/checksum_raw_2.csv \
        integration_tests/seeds/scd2_materialization/checksum_seeds.yml \
        integration_tests/tests/scd2_materialization/checksum_scd2_matches_business_cols.sql \
        integration_tests/tests/scd_materialization/checksum_scd0_matches_business_cols.sql \
        integration_tests/tests/scd_materialization/checksum_scd1_matches_business_cols.sql
git commit -m "feat: add optional _checksum content-fingerprint column (all SCD types)"
```

---

### Task 2: Document `_checksum` in the README

**Files:**
- Modify: `README.md`

**Interfaces:**
- Consumes: `track_checksum` / `checksum_column` option names and defaults from Task 1.

- [ ] **Step 1: Add the two options to the Core Options table**

In `README.md`, in the "Core Options" table, find the `changed_columns_column` row (added by the previous feature):

```markdown
| `changed_columns_column` | ❌ | `_CHANGED` | Name of the change-map object column |
```

Insert immediately after it:

```markdown
| `track_checksum` | ❌ | `false` | Add a `_CHECKSUM` md5 content fingerprint of the business columns (all SCD types) |
| `checksum_column` | ❌ | `_CHECKSUM` | Name of the checksum column |
```

- [ ] **Step 2: Add a "Content Checksum" subsection**

In `README.md`, insert this section immediately before the `## Deletion Support` heading:

````markdown
## Content Checksum

An optional `_checksum` column emits an md5 content fingerprint of the row's business
columns, using the same `generate_surrogate_key` hash the wider platform uses for staging
`_checksum`. It is off by default, enabled per model, and available on all SCD types.

```sql
{{
  config(
    materialized='scd',
    unique_key=['customer_id'],
    meta={'scd_type': 2, 'track_checksum': true}
  )
}}
```

- The fingerprint covers all business columns **including the natural key**, and excludes
  the SCD audit columns and the lifecycle columns (`updated_at`, `created_at`,
  `deleted_at`). Columns are hashed in alphabetical order, so `_checksum` is stable
  regardless of select-list order.
- Two rows with the same `_checksum` have identical business content. On type 0 it is set
  once for the retained row; on type 1 it is recomputed when the row is overwritten; on
  type 2 each version carries its own.

**Limitation:** the column set is derived automatically and is cast to `varchar` for the
hash, so non-scalar columns (`ARRAY` / `OBJECT` / `VARIANT` / `GEOGRAPHY`) in scope may
error or hash non-deterministically. Keep such columns out of the model, or out of scope
for the checksum, if you enable this.
````

- [ ] **Step 3: Commit**

```bash
cd /Users/henry.upton/PycharmProjects/dbt-scd2-utils
git add README.md
git commit -m "docs: document _checksum content-fingerprint column"
```

---

## Self-Review

**1. Spec coverage:**
- Opt-in, default off, meta/vars: Step 6. Covered.
- All SCD types: Steps 6-12 (scd_plan shared + all six builders). Covered.
- Column set (business incl. key, minus audit + lifecycle, alphabetical): Step 6. Covered.
- generate_surrogate_key/md5 parity: Step 5. Covered.
- Per-type write behavior: type 0 Step 11, type 1 Step 12 (incl. matched-update recompute), type 2 Steps 9-10 (per-version + `redundant_versions` placeholder, not in `merge_update_cols`). Covered.
- Ordering (after change_type, before previous/changed): Steps 9-10. Covered.
- Testing (all types, type-1 overwrite, determinism via non-alphabetical select order): Steps 1-3 (assets), 13-14 (runs). Covered.
- Default-off unaffected: Step 15. Covered.
- Docs incl. non-scalar limitation: Task 2. Covered.

**2. Placeholder scan:** No TBD/TODO; every code and test block is complete; every command has expected output.

**3. Type consistency:** `get_checksum_sql(checksum_columns)` signature is identical at definition (Step 5) and every call site (Steps 9-12). arg_dict keys `track_checksum` / `checksum_column` / `checksum_columns` are written in Steps 7-8 and read verbatim in Steps 9-12. `checksum_columns` is computed once (Step 6, sorted) and only read thereafter. `_checksum` is a `varchar` everywhere (`generate_surrogate_key`, `cast(null as varchar)`). The `_checksum` column sits after `_change_type` in `audit_columns` (Step 6 append precedes the type-2 `_previous`/`_changed` appends) and in every builder's select, keeping the type-2 `union all` aligned.
