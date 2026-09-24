# dbt SCD2 Utils

A dbt package providing custom materializations for Slowly Changing Dimension (SCD) tables in Snowflake — full temporal history (Type 2) and current-snapshot (Type 1), with a shared audit-column signature.

[![dbt Hub](https://img.shields.io/badge/dbt-Hub-FF6849)](https://hub.getdbt.com)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)
[![dbt Version](https://img.shields.io/badge/dbt-%3E%3D1.0.0-orange.svg)](https://docs.getdbt.com)

## Features

- **Generic SCD Materialization**: `scd` with `scd_type` (0, 1 or 2) — insert-only, current-snapshot, and full-history all share the same audit columns
- **Custom SCD2 Materialization**: `incremental_scd2` materialization with automatic versioning
- **Snowflake Optimized**: Native MERGE statements and TIMESTAMP_TZ types
- **Automatic Audit Columns**: `_IS_CURRENT`, `_VALID_FROM`, `_VALID_TO`, `_CHANGE_TYPE`
- **Deletion Support**: Optional `deleted_at_column` for logical deletions and resurrections
- **Metadata-Preserving Full Refresh**: `--full-refresh` truncates and reloads in place when the column set is unchanged, so grants, comments, tags, policies and clustering survive
- **Temporal Joins**: `scd2_join` macro with composite key support
- **Content Fingerprint**: hooks that record whether a build changed a table's content, and a guard a materialization can ask before rebuilding a child of `unchanged` parents
- **Configurable**: Customize column names and behavior per model or globally
- **Generic Tests**: Comprehensive SCD2 data quality tests included

## Installation

Add to your `packages.yml`:

```yaml
packages:
  - package: henryupton/dbt-scd2-utils
    version: ["1.0.54"]
```

Then run:
```bash
dbt deps
```

## Quick Start

```sql
-- models/customers_scd2.sql
{{
  config(
    materialized='incremental_scd2',
    unique_key=['customer_id']
  )
}}

select
    customer_id,
    name,
    email,
    updated_at as _updated_at
from {{ source('raw', 'customers') }}
```

### Output

| customer_id | name | email | _is_current | _valid_from | _valid_to | _change_type |
|-------------|------|-------|-------------|-------------|-----------|--------------|
| 123 | John | john@old.com | false | 2023-01-01 | 2023-06-15 | I |
| 123 | John | john@new.com | true | 2023-06-15 | 2999-12-31 | U |

## SCD Types

Use the generic `scd` materialization and pick a type with `scd_type` (defaults to `2`). All types emit the same audit columns (`_is_current`, `_valid_from`, `_valid_to`, `_change_type`), so every dimension shares one table signature regardless of type.

| | Type 0 | Type 1 | Type 2 (default) |
|---|---|---|---|
| Rows per key | Exactly one (immutable) | Exactly one (current snapshot) | One per version (full history) |
| On change | No action (original retained) | Overwrite in place | Expire old version, insert new |
| Merge | `unique_key`, insert only | `unique_key`, upsert | `unique_key` + `updated_at` |
| `_is_current` | always `true` | always `true` | `true` only on latest version |
| `_valid_from` | first-seen, fixed | first-seen, preserved | start of each version |
| `_valid_to` | always `default_valid_to` | always `default_valid_to` | next version's start, else `default_valid_to` |
| `_change_type` | always `I` | always `I` | `I` / `U` / `D` |
| `deleted_at_column` | not supported | not supported | supported |

Types 0 and 1 do no change detection, so `change_columns` and `deleted_at_column` don't apply (setting `deleted_at_column` raises a compiler error).

### SCD Type 1

Efficient for high-cardinality dimensions that don't change much over time — `dim_page`, `dim_session` — where you want one row per entity but still want the audit columns for a consistent signature:

```sql
-- models/dim_page.sql
{{
  config(
    materialized='scd',
    scd_type=1,
    unique_key=['page_id']
  )
}}

select
    page_id,
    url,
    page_type,
    updated_at as _updated_at
from {{ source('raw', 'pages') }}
```

Type 1 does a straight `MERGE` on the business key: new keys are inserted and existing keys are overwritten with the latest values, with the audit columns kept consistent (`_is_current = true`, `_change_type = 'I'`, `_valid_from` preserved).

### SCD Type 0

Insert-only: the original (first-seen) value is retained and never updated. Identical to Type 1 except the merge has no `when matched` clause, so existing keys are left untouched. Useful for write-once reference data.

```sql
{{ config(materialized='scd', scd_type=0, unique_key=['page_id']) }}
```

> **Note:** As with any incremental model, filter your source with `is_incremental()` so each run only processes new/changed rows. Pointing Type 0 or 1 at an unfiltered full-table source will re-scan every row on every run.

### SCD Type 2

`scd_type=2` (the default) gives the full temporal history described throughout this README. The original `incremental_scd2` materialization is retained as a backwards-compatible alias — it behaves identically to `scd` with `scd_type=2`, so existing models need no changes.

```sql
-- equivalent
{{ config(materialized='incremental_scd2', unique_key=['customer_id']) }}
{{ config(materialized='scd', scd_type=2, unique_key=['customer_id']) }}
```

## Configuration

> **Package options live under `meta`.** dbt deprecates custom keys passed directly to `config()` (the `CustomKeyInConfigDeprecation` warning), so this package reads its options from the model's `meta` block. Pass package options inside `meta={...}` in your `config()` call — native keys like `materialized` and `unique_key` stay at the top level — and set global defaults via `vars` (see [Global Configuration](#global-configuration)).

### Core Options

| Option | Required | Default | Description |
|--------|----------|---------|-------------|
| `scd_type` | ❌ | `2` | SCD type for the `scd` materialization (`0`, `1` or `2`) |
| `unique_key` | ✅ | - | Business key columns (array) |
| `change_columns` | ❌ | all columns | Object with `include` and `exclude` arrays to control which columns trigger changes |
| `scd_check_columns` | ❌ | all columns | **(Legacy)** Columns to track for changes |
| `exclude_columns_from_change_check` | ❌ | `[]` | **(Legacy)** Columns to exclude from change tracking |
| `deleted_at_column` | ❌ | none | Column for logical deletion tracking |
| `track_previous_version` | ❌ | `false` | (SCD2 only) add an OBJECT column with the prior version's tracked columns |
| `previous_version_column` | ❌ | `_PREVIOUS` | Name of the previous-version object column |
| `track_changed_columns` | ❌ | `false` | (SCD2 only) add an OBJECT column of per-tracked-column change booleans |
| `changed_columns_column` | ❌ | `_CHANGED` | Name of the change-map object column |
| `track_checksum` | ❌ | `false` | Add a `_CHECKSUM` native-UUID content fingerprint of the business columns (all SCD types) |
| `checksum_column` | ❌ | `_CHECKSUM` | Name of the checksum column |
| `checksum_exclude` | ❌ | `[]` | Columns to omit from the `_checksum` fingerprint (e.g. volatile processing timestamps) |
| `assume_keys_not_null` | ❌ | inferred | (SCD2) `true` matches keys with `=` (prune- and Search-Optimization-eligible); `false` uses null-safe `equal_null`. Unset infers from `not_null` constraints, else guards at runtime. See [Key Matching](#key-matching--search-optimization). |
| `search_optimization` | ❌ | `false` | (SCD2) `true` adds Snowflake Search Optimization (`EQUALITY`) on the key columns. Enterprise Edition; see [Key Matching](#key-matching--search-optimization). |
| `search_optimization_columns` | ❌ | `unique_key` | (SCD2) Columns to build `search_optimization` on, when enabled. |
| `full_refresh_strategy` | ❌ | `truncate` | How `--full-refresh` rebuilds an existing table: `truncate` keeps the table and swaps its rows when the schema is unchanged, `replace` always runs `create or replace table` (see [Full Refresh](#full-refresh)) |

### Audit Column Names

| Option | Default |
|--------|---------|
| `is_current_column` | `_IS_CURRENT` |
| `valid_from_column` | `_VALID_FROM` |
| `valid_to_column` | `_VALID_TO` |
| `updated_at_column` | `_UPDATED_AT` |
| `change_type_column` | `_CHANGE_TYPE` |

Override per model inside `meta` (e.g. `meta={'is_current_column': 'current_flag'}`), or set defaults globally via `vars` (below).

### Global Configuration

Set defaults in `dbt_project.yml`:

```yaml
vars:
  dbt_scd2_utils:
    is_current_column: "current_flag"
    valid_from_column: "eff_start_date"
    valid_to_column: "eff_end_date"
    default_valid_to: "2999-12-31 23:59:59"
    suppress_date_type_warning: false   # default
```

| Var | Default | Behaviour |
|-----|---------|-----------|
| `suppress_date_type_warning` | `false` | The materialization warns when the `updated_at` column is a `DATE` rather than a `TIMESTAMP`, since date-grain change tracking can produce imprecise validity windows. Set to `true` to silence that warning when a `DATE` grain is intentional. Only the warning is suppressed; the DATE handling itself is unchanged. |

### Full Refresh

A `--full-refresh` (or a run where the table does not exist yet) rebuilds the whole table from
the initial-load SQL. When the table already exists and its column set is unchanged, the
package keeps the table object and swaps only its rows:

```sql
begin;
truncate table analytics.dim_customers;
insert into analytics.dim_customers (...) select ... from (...);
commit;
```

Snowflake treats `TRUNCATE` as DML, so this is one transaction: if the insert fails the truncate
rolls back and the old rows are still there. Because the table is never dropped, everything
attached to it survives: grants, comments, tags, masking and row access policies, clustering
keys and Search Optimization. `create or replace table` throws all of that away.

The package falls back to `create or replace table` (the previous behaviour) when:

- the relation does not exist yet, or exists as something other than a table (e.g. a view);
- a business column was added, removed or changed type compared with the freshly built source
  data. A `VARCHAR` that got narrower still fits and counts as unchanged; one that got wider
  forces a replace;
- an audit column is missing from the existing table (e.g. `track_checksum` was just enabled);
- `full_refresh_strategy` is `replace`.

The model's log line says which path was taken and, for a replace, why. Audit column types are
checked by name only, since the package fixes them, so a package upgrade that changes an audit
column's type needs a one-off `--full-refresh` with `full_refresh_strategy: replace` (model
`meta`, or the `dbt_scd2_utils.full_refresh_strategy` var).

### Out-of-Order & Backfill Handling

These project-level vars control how the SCD Type 2 materialization reconciles existing
history when records arrive out of chronological order (for example a backfill carrying an
`updated_at` earlier than rows already in the table). Both default to the safe behaviour.

```yaml
vars:
  dbt_scd2_utils:
    update_all_previous_records: true   # default
    collapse_redundant_versions: true   # default
```

| Var | Default | Behaviour |
|-----|---------|-----------|
| `update_all_previous_records` | `true` | Re-evaluate every existing version of an affected key on each run, so out-of-order arrivals are slotted in correctly. Set to `false` only if data is guaranteed to arrive in chronological order — it is a performance optimisation that otherwise risks multiple `is_current` rows for a key. |
| `collapse_redundant_versions` | `true` | When an out-of-order arrival has tracked columns identical to an existing version, the two collapse into one content run and the **earliest-loaded** row survives (by `loaded_at_column`, default `_loaded_at`; `updated_at` order when the model has no such column). A bulk reload that re-delivers an earlier-dated copy of content that already arrived therefore never back-dates the version or its predecessor's `valid_to`. With the default the now-redundant row is **deleted**, so an incremental run matches a full refresh, and the initial load applies the same survivor rule, so a full refresh followed by an incremental run over the same input is a no-op. Set to `false` to **keep** the redundant version instead (no deletes; the existing row is still correctly re-expired). Only takes effect when `update_all_previous_records` is also `true`. |

### Key Matching & Search Optimization

The SCD2 incremental MERGE and its `previous_record` lookup match the target on the business
key. By default keys are matched with plain `=`, which lets Snowflake prune the history table by
key (and is eligible for the Search Optimization Service). A key column that can be `NULL` needs
null-safe `equal_null` instead (`NULL = NULL` is UNKNOWN under `=`, which would leave a
null-bearing key's prior version un-expired and duplicate its current row). The operator is
resolved per model, cheapest first:

1. **`assume_keys_not_null` config** (model `meta` or global `vars`): `true` forces `=`, `false`
   forces `equal_null`.
2. **Declared constraints**: if every `unique_key` column has a `not_null` constraint (schema.yml
   / contract), `=` is used. Snowflake enforces `NOT NULL` (the only enforced constraint), so this
   is a real guarantee.
3. **Runtime guard** (default when neither is set): the incoming batch is checked for NULLs in the
   key. If none, `=` is used. If any, the model **warns and falls back to `equal_null`** for that
   run, so output is always correct. Set `assume_keys_not_null: false` to skip the check and the
   warning when you know a key is nullable.

Because `=` and `equal_null` are identical when there are no NULLs, this default is
behaviour-preserving: clean-key models simply gain prune-friendly matching.

**Search Optimization** (`search_optimization`, opt-in, **Enterprise Edition**) has the package
run `ALTER TABLE ... ADD SEARCH OPTIMIZATION ON EQUALITY(...)` on the key columns after every
`--full-refresh` / initial load. The path survives truncate + insert (see [Full Refresh](#full-refresh))
and incremental merges, and re-adding an existing column is a no-op in Snowflake, so the ALTER is
simply re-asserted on each full refresh; enabling it on an existing model takes effect on the
next full refresh. `ADD` is additive, so dropping a column from `search_optimization_columns`
does not remove its path: run one full refresh with `full_refresh_strategy: replace`, or
`ALTER TABLE ... DROP SEARCH OPTIMIZATION` by hand.
Search Optimization only accelerates `=`/`IN` predicates, so it does nothing while a model falls
back to `equal_null` (the package warns if you enable it on a nullable-key model). It carries
ongoing storage and maintenance cost; verify the benefit to your merge with `EXPLAIN` before
relying on it.

```sql
{{
  config(
    materialized='incremental_scd2',
    unique_key=['customer_id'],
    meta={'assume_keys_not_null': true, 'search_optimization': true}
  )
}}
```

### Change Column Configuration

Control which columns trigger SCD2 changes using the `change_columns` object:

```sql
{{
  config(
    materialized='incremental_scd2',
    unique_key=['customer_id'],
    meta={
      'change_columns': {
        'include': ['customer_name', 'email', 'status'],
        'exclude': ['last_login_at', '_metadata']
      }
    }
  )
}}

select
    customer_id,
    customer_name,
    email,
    status,
    last_login_at,  -- excluded: won't trigger new SCD2 versions
    _metadata,      -- excluded: won't trigger new SCD2 versions
    updated_at as _updated_at
from {{ source('raw', 'customers') }}
```

**How it works:**
- **`include`**: Explicitly specify which columns should be tracked for changes
  - If provided, only these columns will trigger new SCD2 versions
  - Automatically filters to columns that exist in your table
  - Still excludes any columns in the `exclude` array

- **`exclude`**: Specify columns to ignore when detecting changes
  - Useful for metadata or system columns that change frequently
  - The `updated_at_column` is automatically excluded (always)
  - Works with or without the `include` array

**Examples:**

```sql
-- Track only specific columns
{{
  config(
    materialized='incremental_scd2',
    unique_key=['product_id'],
    meta={
      'change_columns': {
        'include': ['name', 'price', 'description']
      }
    }
  )
}}

-- Exclude specific columns (track all others)
{{
  config(
    materialized='incremental_scd2',
    unique_key=['order_id'],
    meta={
      'change_columns': {
        'exclude': ['_synced_at', '_batch_id']
      }
    }
  )
}}

-- Combine both approaches
{{
  config(
    materialized='incremental_scd2',
    unique_key=['user_id'],
    meta={
      'change_columns': {
        'include': ['name', 'email', 'role', 'department'],
        'exclude': ['last_seen_at']  -- even if in include, this will be excluded
      }
    }
  )
}}
```

**Backwards Compatibility:**

The legacy configuration options are still supported:
- `scd_check_columns`: equivalent to `change_columns.include`
- `exclude_columns_from_change_check`: equivalent to `change_columns.exclude`

If you use the new `change_columns` object, it takes precedence over the legacy options. Both approaches work identically:

```sql
-- New approach (recommended)
meta={'change_columns': {'include': ['name', 'email'], 'exclude': ['metadata']}}

-- Legacy approach (still supported)
meta={
  'scd_check_columns': ['name', 'email'],
  'exclude_columns_from_change_check': ['metadata']
}
```

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

**Enabling on an existing table:** turning a switch on adds a new column, so run a one-off
`--full-refresh` when you enable it on an already-built model. Without it the next
incremental run errors with an invalid-identifier on the new column (the same requirement
as adding `deleted_at_column` to an existing model).

## Content Checksum

An optional `_checksum` column emits a native-UUID content fingerprint of the row's business
columns. The `generate_surrogate_key` md5 is reformatted to `8-4-4-4-12` and cast to a UUID
via `to_uuid`, matching how the wider platform builds its surrogate keys and staging
`_checksum`. It is off by default, enabled per model, and available on all SCD types.
Declare `data_type: uuid` for the column in any enforced-contract model.

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
- The column set is derived automatically, so any **volatile column the model emits** (an
  ingestion timestamp such as `_written_at` or `_loaded_at`, a `sysdate()` value, a batch
  id) is folded into the fingerprint and makes `_checksum` differ for otherwise-identical
  content. List such columns under `checksum_exclude` (case-insensitive) to keep the
  "same checksum means same content" guarantee:

  ```sql
  meta={'track_checksum': true, 'checksum_exclude': ['_written_at']}
  ```

- Enabling it adds a column, so run a one-off `--full-refresh` when you turn it on for an
  already-built model, otherwise the next incremental run errors on the new column (the same
  requirement as `track_previous_version` / `track_changed_columns` and `deleted_at_column`).

**Limitation:** the remaining column set is cast to `varchar` for the hash, so non-scalar
columns (`ARRAY` / `OBJECT` / `VARIANT` / `GEOGRAPHY`) still in scope may error or hash
non-deterministically. Keep such columns out of the model, or list them in
`checksum_exclude`, if you enable this.

## Deletion Support

Track logical deletions and resurrections:

```sql
{{
  config(
    materialized='incremental_scd2',
    unique_key=['product_id'],
    meta={
      'deleted_at_column': 'deleted_at'
    }
  )
}}

select
    product_id,
    name,
    price,
    deleted_at,
    updated_at as _updated_at
from {{ source('raw', 'products') }}
```

### Deletion Behavior

- **Deletion record**: `_change_type = 'D'`, `_valid_from = deleted_at`
- **Resurrection**: Next record after deletion has `_change_type = 'I'`
- **Valid_to**: Extends to next record or end of time (not set to deleted_at)

### Example

| product_id | name | deleted_at | _change_type | _valid_from | _valid_to |
|------------|------|------------|--------------|-------------|-----------|
| 1 | Widget | null | I | 2024-01-01 | 2024-01-10 |
| 1 | Widget | 2024-01-10 | D | 2024-01-10 | 2024-01-15 |
| 1 | Widget | null | I | 2024-01-15 | 2999-12-31 |

## Temporal Joins

Join multiple SCD2 tables across time with composite key support:

```sql
-- models/customer_orders_history.sql
{{
  config(materialized='table')
}}

{{ dbt_scd2_utils.scd2_join(
    [ref('customers_scd2'), ref('orders_scd2')],
    ['customer_id']
) }}

-- Composite keys
{{ dbt_scd2_utils.scd2_join(
    [ref('orders_scd2'), ref('order_items_scd2')],
    ['customer_id', 'order_id']
) }}
```

The macro creates a temporal spine and joins all tables' active versions for each time period.

## Content Fingerprint

Tools for answering "did this build change the table's content?" so a deploy can skip descendants whose upstream came out identical. They are general macros under `macros/fingerprint/`; nothing in the `scd` materializations calls them, and a project opts in with two hooks and a var.

```yaml
# dbt_project.yml
on-run-start:
  - "{{ dbt_scd2_utils.fingerprint_register() }}"

models:
  +post-hook: "{{ dbt_scd2_utils.fingerprint_post(this) }}"
```

```bash
dbt build --vars '{"fingerprint": true, "deploy_id": "deploy-123"}'
```

`fingerprint_register()` writes one `pending` row per selected model, seed and snapshot into a ledger table (`fingerprint_deploy_node`), stamped with Snowflake's clock as `snapshot_at`, the node's parents and its column shape. `fingerprint_post(this)` then compares the table as built with the table `at(timestamp => snapshot_at)` and records a verdict:

| Verdict | Meaning | Blocks a child's skip |
|---------|---------|-----------------------|
| `new` | Object created or replaced after the snapshot, or a view | yes |
| `unchanged` | Nothing written and the row count is equal, or every hashed month is equal | no |
| `appended` | Every row the build wrote sits above the pre-build watermark (`max(_loaded_at)`) | no |
| `modified` | A column was added, removed or retyped, or a month at or below the watermark counts or hashes differently | yes |
| `unhashable` | No loaded-at column on one side | yes |
| `error` | Relation missing, or no Time Travel (retention 0) | yes |
| `skipped` | The guard skipped the build | no |

The comparison is cheap until it has to hash. Object state comes from `show tables like`, shape from `show columns`, and the counts are filtered scalar subqueries on `METADATA$ROW_LAST_COMMIT_TIME` and the watermark, which prune to the partitions the build wrote. Only then are the touched `_loaded_at` months hashed with `hash_agg` on both sides (every month when every row was rewritten), and per-month row counts catch a deletion in a month the build never touched. Per-month detail lands in `fingerprint_deploy_segment`.

`fingerprint_should_skip()` is for a materialization to call before building. It returns true only when `fingerprint_skip_unchanged_upstream` is on, at least one parent is registered in this deploy, and no registered parent is `pending`, `new`, `modified`, `unhashable` or `error`. A node with no registered parents always builds. `fingerprint_mark_skipped()` records the skip so the node's own children can read it. `integration_tests/macros/guarded_table.sql` shows the shape:

```jinja
{% if existing_relation is not none and dbt_scd2_utils.fingerprint_should_skip() %}
  {% do dbt_scd2_utils.fingerprint_mark_skipped(detail='no blocking parent verdict') %}
  {% call statement('main') %}select 'skipped' as outcome{% endcall %}
  {{ return({'relations': [existing_relation]}) }}
{% endif %}
```

| Var | Default | Purpose |
|-----|---------|---------|
| `fingerprint` | `false` | Master switch; every macro is a no-op without it |
| `fingerprint_skip_unchanged_upstream` | `false` | Lets `fingerprint_should_skip()` return true |
| `deploy_id` | `invocation_id` | Shared by the steps of one deploy; registration is idempotent per deploy id |
| `fingerprint_schema` | `target.schema` | Schema holding the two ledger tables |
| `fingerprint_loaded_at_column` | `_loaded_at` | Watermark and month-bucket column; per model via `meta.fingerprint_loaded_at` |
| `fingerprint_exclude` | `_batched_at`, `_written_at`, `_synthesised_at` | Columns left out of the hash, plus the package's `is_current_column` and `valid_to_column`; per model via `meta.fingerprint_exclude` |
| `fingerprint_select_tag` | none | Fallback selection for runtimes without `selected_resources` |

Requirements: Snowflake with Time Travel on the fingerprinted tables (transient tables cap at one day, which is enough for a deploy) and row commit timestamps (`ROW_TIMESTAMP_DEFAULT` or per-table `ROW_TIMESTAMP`). A build has to keep the object for the snapshot to survive: truncate + insert, `insert overwrite`, `merge` and `insert` all do; `create or replace` reads `new`, which is the right verdict for a first build or a column-set change.

## Generic Tests

Apply comprehensive SCD2 validation tests:

```yaml
# models/schema.yml
models:
  - name: customers_scd2
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

      - dbt_scd2_utils.insert_follows_delete:
          arguments:
            key_columns: [customer_id]
            change_type_column: _change_type
            valid_from_column: _valid_from

      - dbt_scd2_utils.no_consecutive_inserts_or_deletes:
          arguments:
            key_columns: [customer_id]
            change_type_column: _change_type
            valid_from_column: _valid_from
```

### Available Tests

- `one_current_per_key`: One current record per key
- `no_validity_overlaps`: No overlapping validity windows
- `continuous_validity_windows`: No gaps in validity periods
- `latest_row_is_current`: Latest record marked as current
- `first_record_not_update`: A key's first record is an insert or delete, never an update
- `insert_follows_delete`: Resurrections marked as 'I'
- `no_consecutive_inserts_or_deletes`: Valid change type sequences
- `no_records_after_deletion`: Deletion records have correct valid_from
- `all_records_current`: Every row is current (SCD Type 1 invariant)
- `valid_window_well_formed`: `valid_from` is before `valid_to`, neither null (all types)

## Change Types

| Type | Description | When Applied |
|------|-------------|--------------|
| `I` | Insert | First record or after deletion (resurrection) |
| `U` | Update | Subsequent changes to existing records |
| `D` | Delete | Record has non-null deleted_at_column |

## Requirements

- **dbt**: >= 1.0.0
- **Database**: Snowflake
- **Adapter**: dbt-snowflake
- **Dependencies**: dbt-utils (auto-installed)

## Testing

```bash
# Run all tests
dbt test

# Run integration tests
cd integration_tests && dbt build
```

## License

Apache License 2.0 - see [LICENSE](LICENSE) file for details.

## Support

- **Issues**: [GitHub Issues](https://github.com/henryupton/dbt-scd2-utils/issues)
- **dbt Community**: [dbt Slack](https://getdbt.slack.com)