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
- **Temporal Joins**: `scd2_join` macro with composite key support
- **Configurable**: Customize column names and behavior per model or globally
- **Generic Tests**: Comprehensive SCD2 data quality tests included

## Installation

Add to your `packages.yml`:

```yaml
packages:
  - package: henryupton/dbt-scd2-utils
    version: ["1.0.38"]
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

### Core Options

| Option | Required | Default | Description |
|--------|----------|---------|-------------|
| `scd_type` | ❌ | `2` | SCD type for the `scd` materialization (`0`, `1` or `2`) |
| `unique_key` | ✅ | - | Business key columns (array) |
| `change_columns` | ❌ | all columns | Object with `include` and `exclude` arrays to control which columns trigger changes |
| `scd_check_columns` | ❌ | all columns | **(Legacy)** Columns to track for changes |
| `exclude_columns_from_change_check` | ❌ | `[]` | **(Legacy)** Columns to exclude from change tracking |
| `deleted_at_column` | ❌ | none | Column for logical deletion tracking |

### Audit Column Names

| Option | Default |
|--------|---------|
| `is_current_column` | `_IS_CURRENT` |
| `valid_from_column` | `_VALID_FROM` |
| `valid_to_column` | `_VALID_TO` |
| `updated_at_column` | `_UPDATED_AT` |
| `change_type_column` | `_CHANGE_TYPE` |

### Global Configuration

Set defaults in `dbt_project.yml`:

```yaml
vars:
  dbt_scd2_utils:
    is_current_column: "current_flag"
    valid_from_column: "eff_start_date"
    valid_to_column: "eff_end_date"
    default_valid_to: "2999-12-31 23:59:59"
```

### Change Column Configuration

Control which columns trigger SCD2 changes using the `change_columns` object:

```sql
{{
  config(
    materialized='incremental_scd2',
    unique_key=['customer_id'],
    change_columns={
      'include': ['customer_name', 'email', 'status'],
      'exclude': ['last_login_at', '_metadata']
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
    change_columns={
      'include': ['name', 'price', 'description']
    }
  )
}}

-- Exclude specific columns (track all others)
{{
  config(
    materialized='incremental_scd2',
    unique_key=['order_id'],
    change_columns={
      'exclude': ['_synced_at', '_batch_id']
    }
  )
}}

-- Combine both approaches
{{
  config(
    materialized='incremental_scd2',
    unique_key=['user_id'],
    change_columns={
      'include': ['name', 'email', 'role', 'department'],
      'exclude': ['last_seen_at']  -- even if in include, this will be excluded
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
change_columns={'include': ['name', 'email'], 'exclude': ['metadata']}

-- Legacy approach (still supported)
scd_check_columns=['name', 'email'],
exclude_columns_from_change_check=['metadata']
```

## Deletion Support

Track logical deletions and resurrections:

```sql
{{
  config(
    materialized='incremental_scd2',
    unique_key=['product_id'],
    deleted_at_column='deleted_at'
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