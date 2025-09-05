# Integration Tests for dbt_scd2_utils

This directory contains integration tests for the dbt-scd2-utils package, organized by functionality.

## Directory Structure

### Models

#### `models/scd2_materialization/`
Tests for the core SCD2 materialization functionality:
- **`customers_scd2.sql`** - Main SCD2 materialization test model
- **`schema.yml`** - Tests and configuration for SCD2 models

#### `models/source_macro/`
Tests for the enhanced `source()` macro functionality:
- **`test_source_macro_basic.sql`** - Basic source macro usage (no loaded_at parameter)
- **`test_source_macro_with_loaded_at.sql`** - Source macro with loaded_at parameter but feature disabled
- **`test_source_macro_exclude_data.sql`** - Source macro with exclude_data_after_run_start enabled
- **`test_source_macro_incremental.sql`** - Source macro with incremental materialization + exclude_data_after_run_start

#### `models/unit_tests/`
Unit test models and configurations:
- **`test_incremental_behavior.sql`** - Tests the is_incremental() macro override
- **`test_source_incremental_behavior.sql`** - Tests the source() macro in unit test context
- **`schema.yml`** - Unit test definitions
- **`sources.yml`** - Source definitions for unit tests

### Seeds

#### `seeds/scd2_materialization/`
Test data for SCD2 materialization tests:
- **`customers_raw_1.csv` through `customers_raw_5.csv`** - Sequential customer data for testing SCD2 incremental behavior
- **`schema.yml`** - Column definitions and data types

#### `seeds/source_macro/`
Test data for source macro tests:
- **`test_transactions.csv`** - Transaction data with loaded_at timestamps for testing the exclude_data_after_run_start feature
- **`raw_orders.csv`** - Order data for source macro testing

#### `seeds/unit_tests/`
Test data for unit tests:
- **`unit_test_customers_input.csv`** - Input data for unit testing scenarios

## Running Tests

### All Tests
```bash
dbt deps    # Install dependencies
dbt seed    # Load test data
dbt run     # Run all models
dbt test    # Run all tests
```

### By Category

#### Unit Tests
```bash
dbt test --select test_type:unit
```

#### SCD2 Materialization Tests
```bash
dbt seed --select seeds/scd2_materialization/
dbt run --select models/scd2_materialization/
dbt test --select models/scd2_materialization/
```

#### Source Macro Tests
```bash
dbt seed --select seeds/source_macro/
dbt run --select models/source_macro/
```

#### Specific Feature Testing
```bash
# Test exclude_data_after_run_start feature
dbt run --select test_source_macro_exclude_data --vars "exclude_data_after_run_start: true"

# Test incremental + exclude_data_after_run_start
dbt run --select test_source_macro_incremental --vars "exclude_data_after_run_start: true"
```

## Features Tested

- ✅ **SCD2 Materialization**: Core slowly changing dimension type 2 logic
- ✅ **Enhanced Source Macro**: Incremental loading with `loaded_at_col` parameter
- ✅ **Exclude Data After Run Start**: Maintains data consistency across dbt runs
- ✅ **Unit Tests**: Validates macro behavior in isolated test scenarios
- ✅ **Integration Tests**: End-to-end testing with real data scenarios

## Test Scenarios

### SCD2 Materialization
1. **Initial Load**: Verify SCD2 table creation with proper audit columns
2. **Incremental Updates**: Test that changed records create new versions and expire old ones
3. **Unchanged Records**: Verify unchanged records remain untouched
4. **New Records**: Test insertion of completely new records

### Source Macro
1. **Basic Usage**: Source macro without loaded_at parameter returns full table
2. **Loaded At Column**: Source macro with loaded_at but feature disabled returns full table
3. **Exclude After Run Start**: Source macro filters data arriving after run start
4. **Incremental + Exclude**: Combines incremental loading with run-start filtering