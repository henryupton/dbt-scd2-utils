# dbt SCD2 Utils

A dbt package providing a custom materialization for Slowly Changing Dimension (SCD) Type 2 tables in Snowflake.

[![dbt Hub](https://img.shields.io/badge/dbt-Hub-FF6849)](https://hub.getdbt.com)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)
[![dbt Version](https://img.shields.io/badge/dbt-%3E%3D1.0.0-orange.svg)](https://docs.getdbt.com)

## Features

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
    version: ["1.0.22"]
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

## Configuration

### Core Options

| Option | Required | Default | Description |
|--------|----------|---------|-------------|
| `unique_key` | ✅ | - | Business key columns (array) |
| `scd_check_columns` | ❌ | all columns | Columns to track for changes |
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
- `first_record_insert`: First records have change_type = 'I'
- `insert_follows_delete`: Resurrections marked as 'I'
- `no_consecutive_inserts_or_deletes`: Valid change type sequences
- `no_records_after_deletion`: Deletion records have correct valid_from

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