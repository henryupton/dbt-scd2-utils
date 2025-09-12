# dbt SCD2 Utils

A dbt package providing a custom materialization for implementing Slowly Changing Dimension (SCD) Type 2 tables in Snowflake. Built for high-performance SCD2 operations using Snowflake's native MERGE statements and TIMESTAMP_TZ data types.

[![dbt Hub](https://img.shields.io/badge/dbt-Hub-FF6849)](https://hub.getdbt.com)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)
[![dbt Version](https://img.shields.io/badge/dbt-%3E%3D1.0.0-orange.svg)](https://docs.getdbt.com)

## Features

- **🚀 Custom materialization**: `incremental_scd2` materialization specifically designed for SCD Type 2 dimensions
- **⚡ Snowflake optimized**: Uses native MERGE statements and TIMESTAMP_TZ for optimal performance
- **🔄 Automatic audit columns**: Manages `_IS_CURRENT`, `_VALID_FROM`, `_VALID_TO`, `_UPDATED_AT`, and `_CHANGE_TYPE`
- **📊 Direct column comparison**: Efficient change detection by comparing actual column values
- **⚙️ Configurable**: Customize audit column names and SCD behavior per model
- **🧪 Production ready**: Comprehensive windowing logic handles complex SCD2 scenarios
- **🔗 Temporal joins**: `scd2_join` macro for joining multiple SCD2 tables across time
- **⚡ Incremental source loading**: Enhanced `source()` macro with automatic incremental filtering
- **🎯 Extended incremental support**: Custom `is_incremental()` macro supporting SCD2 materialization
- **📚 Interactive demo**: Complete demo with 6 batches showing real-world SCD2 behavior

## Installation

### Via dbt Package Hub

Add to your `packages.yml`:

```yaml
packages:
  - package: henryupton/dbt-scd2-utils
    version: ["1.0.9"]
```

### Via Git

```yaml
packages:
  - git: "https://github.com/henryupton/dbt-scd2-utils.git"
    revision: 1.0.9
```

Then run:
```bash
dbt deps
```

## Configuration Options

| Option                   | Required | Default                    | Description                                                      |
|--------------------------|----------|----------------------------|------------------------------------------------------------------|
| `unique_key`             | ✅        | -                          | Business key column(s) that uniquely identify a dimension member |
| `scd_check_columns`      | ❌        | all non-audit columns      | List of columns to track for changes                             |
| `incremental_predicates` | ❌        | `[]`                       | Additional WHERE conditions for the MERGE statement              |
| `is_current_column`      | ❌        | `_IS_CURRENT`              | Name of the current record flag column                           |
| `valid_from_column`      | ❌        | `_VALID_FROM`              | Name of the valid from timestamp column                          |
| `valid_to_column`        | ❌        | `_VALID_TO`                | Name of the valid to timestamp column                            |
| `updated_at_column`      | ❌        | `_UPDATED_AT`              | Name of the source system timestamp column                       |
| `change_type_column`     | ❌        | `_CHANGE_TYPE`             | Name of the change type flag column (I, U, D)                    |
| `change_type_expr`       | ❌        | `null`                     | Custom SQL expression for change type detection                  |
| `default_valid_to`       | ❌        | `2999-12-31 23:59:59+0000` | Default valid_to value for current records                       |

## Advanced Usage

### Custom Column Names

```sql
{{ 
  config(
    materialized='incremental_scd2',
    unique_key=['customer_id'],
    is_current_column='current_flag',
    valid_from_column='eff_start_date',
    valid_to_column='eff_end_date',
  ) 
}}
```

### Global Configuration

Set defaults in your `dbt_project.yml`:

```yaml
vars:
  dbt_scd2_utils:
    is_current_column: "current_flag"
    valid_from_column: "eff_start_date" 
    valid_to_column: "eff_end_date"
    default_valid_to: "2999-12-31 23:59:59"
```

### Custom Change Type Detection

Control how change types (I, U, D) are assigned using `change_type_expr`:

#### Default Behavior (no change_type_expr)
```sql
{{ 
  config(
    materialized='incremental_scd2',
    unique_key=['customer_id']
  ) 
}}
```
Uses ROW_NUMBER logic: first occurrence = 'I', subsequent = 'U'

#### Custom Expression
```sql
{{ 
  config(
    materialized='incremental_scd2',
    unique_key=['customer_id'],
    change_type_expr="CASE WHEN status = 'DELETED' THEN 'D' WHEN created_at = updated_at THEN 'I' ELSE 'U' END"
  ) 
}}
```

#### Change Type Values
- **'I'**: Insert (new records)
- **'U'**: Update (changed records or expired versions)  
- **'D'**: Delete (soft deletes detected from source)

### Incremental Predicates

Add conditional logic to your MERGE:

```sql
{{ 
  config(
    materialized='incremental_scd2',
    unique_key=['customer_id'],
    incremental_predicates=["dbt_internal_dest.region = 'US'"]
  ) 
}}
```

### Temporal Joins with `scd2_join`

The `scd2_join` macro enables joining multiple SCD2 tables across time by creating a temporal spine that reconstructs the state of all tables at each point in time when any table changed.

#### Basic Usage

```sql
-- Model: customer_address_history.sql
{{ 
  config(
    materialized='table'
  ) 
}}

{{ dbt_scd2_utils.scd2_join([
    ref('customers_scd2'),
    ref('addresses_scd2')
  ], 'customer_id') }}
```

#### How It Works

1. **Temporal Spine Creation**: Collects all `_valid_from` and `_valid_to` timestamps from all input tables
2. **Time Range Generation**: Creates distinct time periods when any table had changes
3. **Temporal Joins**: Joins each table's active version for each time period
4. **Complete History**: Returns a complete timeline showing how all tables looked together at each point in time

#### Example Output

For customer and address SCD2 tables:

| customer_id | name       | city   | phone    | _is_current | _valid_from | _valid_to  |
|-------------|------------|--------|----------|-------------|-------------|------------|
| 123         | John Smith | Boston | 555-0001 | false       | 2023-01-01  | 2023-03-15 |
| 123         | John Smith | Boston | 555-0002 | false       | 2023-03-15  | 2023-06-01 |  
| 123         | John Smith | NYC    | 555-0002 | true        | 2023-06-01  | 2999-12-31 |

### Enhanced Source Macro

The enhanced `source()` macro adds incremental loading capability to source tables, automatically filtering for new records on incremental runs.

#### Basic Usage

```sql
-- Instead of: select * from {{ source('raw_data', 'customers') }}
-- Use this for incremental loading:
select * from {{ source('raw_data', 'customers', 'updated_at') }}
```

#### Behavior

- **Full Refresh**: Returns complete source table (standard dbt behavior)
- **Incremental Runs**: Automatically filters for records where `loaded_at_col > max(loaded_at_col)` from target table
- **Performance**: Reduces data processing by only loading new/changed records

#### Example in SCD2 Model

```sql
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
from {{ source('raw_data', 'customers', 'updated_at') }}
```

### Extended Incremental Support

The custom `is_incremental()` macro extends dbt's built-in functionality to recognize the `incremental_scd2` materialization type.

#### Enhanced Logic

```sql
-- This now works in incremental_scd2 models:
{% if is_incremental() %}
  -- Custom logic for incremental runs
  where updated_at > (select max(_updated_at) from {{ this }})
{% endif %}
```

#### Supported Materializations

- `incremental` (standard dbt)
- `incremental_scd2` (this package)

## How It Works

### Initial Load (Full Refresh)
1. **Windowing logic**: Uses `ROW_NUMBER()` and `LEAD()` functions to handle multiple versions in initial data
2. **Audit columns**: Automatically adds SCD2 audit columns with proper `TIMESTAMP_TZ` types
3. **Change type detection**: Assigns 'I' for first occurrence, 'U' for subsequent versions per business key

### Incremental Updates
1. **MERGE operation**: Uses Snowflake's native MERGE for optimal performance
2. **Change detection**: Only processes records where tracked columns differ
3. **Version management**: Automatically expires old versions and creates new ones
4. **Temporal logic**: Maintains proper `_VALID_FROM` and `_VALID_TO` ranges

### Generated Audit Columns

| Column         | Type         | Description                                         |
|----------------|--------------|-----------------------------------------------------|
| `_IS_CURRENT`  | BOOLEAN      | Flag indicating if this is the current version      |
| `_VALID_FROM`  | TIMESTAMP_TZ | When this version became effective                  |
| `_VALID_TO`    | TIMESTAMP_TZ | When this version stopped being effective           |
| `_UPDATED_AT`  | TIMESTAMP_TZ | Source system timestamp                             |
| `_CHANGE_TYPE` | VARCHAR(1)   | Change operation type: I(nsert), U(pdate), D(elete) |

## Example Output

For a customer with email changes over time:

| customer_id | name       | email        | _is_current | _valid_from | _valid_to  | _change_type |
|-------------|------------|--------------|-------------|-------------|------------|--------------|
| 123         | John Smith | john@old.com | false       | 2023-01-01  | 2023-06-15 | I            |
| 123         | John Smith | john@new.com | true        | 2023-06-15  | 2999-12-31 | U            |

## Requirements

- **dbt**: >= 1.0.0
- **Database**: Snowflake
- **Adapter**: dbt-snowflake 
- **Dependencies**: dbt-utils (automatically installed)

## Best Practices

1. **Choose appropriate unique keys**: Use stable business keys, not surrogate keys
2. **Select relevant check columns**: Only track columns that matter for your SCD2 logic
3. **Monitor performance**: Large dimension tables may benefit from partitioning
4. **Test thoroughly**: Use the included demo to understand SCD2 behavior
5. **Version control**: Tag your package versions for reproducibility

## Testing

The package includes a comprehensive test suite covering SCD2 behavior, temporal joins, and edge cases.

```bash
# Run all tests
dbt test

# Test specific SCD2 behaviors  
dbt test --select tag:scd2

# Test temporal join functionality
dbt test --select test_scd2_join

# Run the interactive demo
cd integration_tests && dbt run && dbt test

# Run sequential SCD2 testing script
cd integration_tests && ./test_scd2_sequence.sh
```

### Test Coverage

- **SCD2 Basic Functionality**: Validates audit column generation, change detection, and versioning
- **Custom Change Types**: Tests custom `change_type_expr` configuration  
- **Custom Column Names**: Verifies custom audit column naming
- **Temporal Joins**: Tests `scd2_join` macro with multiple SCD2 tables including:
  - Data accuracy and completeness
  - Temporal consistency across joins
  - Expected row count validation
  - Exact output matching

### SCD2 Data Quality Tests

The package includes comprehensive generic tests to validate SCD2 table integrity:

- **`one_current_per_key`**: Ensures exactly one current record per business key
- **`no_validity_overlaps`**: Validates that validity windows don't overlap for the same key  
- **`continuous_validity_windows`**: Checks that validity periods are continuous with no gaps
- **`latest_row_is_current`**: Verifies the chronologically latest record is marked as current
- **`first_record_insert`**: Confirms first records are marked with change type 'I'
- **`last_record_update_or_delete`**: Validates last records use change type 'U' or 'D'

These tests support composite keys and can be applied to any SCD2 table:

```yaml
# models/schema.yml
models:
  - name: your_scd2_table
    tests:
      - dbt_scd2_utils.one_current_per_key:
          key_columns: [customer_id, product_id]  # Supports composite keys
          current_column: _is_current
      - dbt_scd2_utils.no_validity_overlaps:
          key_columns: [customer_id, product_id]
          valid_from_column: _valid_from
          valid_to_column: _valid_to
```

### Sequential Testing Script

The `test_scd2_sequence.sh` script provides automated sequential testing of SCD2 logic across multiple data iterations:

```bash
# Test all iterations (1-5)
./test_scd2_sequence.sh

# Test specific range  
./test_scd2_sequence.sh 1 3

# Test with custom model
./test_scd2_sequence.sh 1 3 my_scd2_model
```

The script tests complex SCD2 scenarios including:
- **Multiple records per key in single batch**: Tests initial loads with multiple versions
- **Out-of-order records**: Validates handling of late-arriving data with earlier timestamps
- **Composite key support**: Works with multi-column business keys
- **Timezone handling**: Tests TIMESTAMP_TZ data types with UTC offsets

## Performance Considerations

- **Snowflake optimization**: Uses `TIMESTAMP_TZ`, MERGE statements, and window functions optimally
- **Column comparison**: Only processes changed records, reducing compute
- **Incremental predicates**: Add filters to limit processing scope
- **Clustering**: Consider clustering on unique key columns for large tables

## Troubleshooting

### Common Issues

1. **Missing unique_key**: Ensure you specify `unique_key` in your model config
2. **Column conflicts**: Audit column names must not exist in your source data
3. **Data types**: Source `_updated_at` should be timestamp compatible
4. **Dependencies**: Ensure dbt-utils is installed via `dbt deps`

### Debug Mode

Enable logging to see generated SQL:

```bash
dbt --log-level debug run --models your_scd2_model
```

## Roadmap

### Planned Features

- **🗑️ Deletion Handling**: Enhanced SCD2 materialization with configurable deletion support
  - `deleted_at_expr` configuration for custom deletion timestamp detection
  - Multiple deletion strategies: end-dating, deletion records, status updates
  - Support for soft deletes and record resurrection scenarios
  - Automatic handling of historical deletions and batch deletions

### TODO

- [ ] Implement `deleted_at_expr` parameter in SCD2 materialization
- [ ] Add deletion handling strategies configuration
- [ ] Create tests for deletion scenarios including resurrection
- [ ] Add documentation and examples for deletion handling
- [ ] Performance optimization for large deletion batches

## Contributing

Contributions are welcome! Please:

1. Fork the repository
2. Create a feature branch
3. Add tests for new functionality
4. Submit a pull request

## License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.

## Support

- **Issues**: [GitHub Issues](https://github.com/your-username/dbt-scd2-utils/issues)
- **Discussions**: [GitHub Discussions](https://github.com/your-username/dbt-scd2-utils/discussions)
- **dbt Community**: [dbt Slack](https://getdbt.slack.com)

---
