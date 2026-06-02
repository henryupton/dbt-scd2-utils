# Integration Tests for dbt_scd2_utils

This directory contains integration tests for the dbt-scd2-utils package, organized by functionality.

## Directory Structure

### Models

#### `models/scd2_materialization/`
Tests for the core SCD2 materialization functionality:
- **`customers_scd2.sql`** - Main SCD2 materialization test model
- **`schema.yml`** - Tests and configuration for SCD2 models

#### `models/scd_materialization/`
Tests for the generic `scd` materialization (types 0 and 1):
- **`customers_scd0.sql`** - SCD type 0 model (insert only, original value retained)
- **`customers_scd1.sql`** - SCD type 1 model (one current row per key, overwritten)
- **`customers_scd1_deleted_at_invalid.sql`** - Disabled negative fixture; `deleted_at_column` on a type 0/1 model must raise a compiler error (see `test_scd_negative.sh`)
- **`schema.yml`** - Invariant tests plus `matches_expected_seed` behavioural checks

Behavioural expectations live in `seeds/scd_materialization/` as `customers_scd{0,1}_expected_{iteration}.csv`; the `matches_expected_seed` test compares the model to the seed for the current `iteration`.

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

#### SCD Types 0 & 1 (generic `scd` materialization)
```bash
# Initial-load behaviour (iteration 1): builds models + runs all tests, including
# the matches_expected_seed checks against customers_scd{0,1}_expected_1.
dbt build --select +models/scd_materialization/

# Incremental behaviour (iterations 1 -> 2): drives successive loads via the
# sequence script, running the tests after each load. Expected seeds exist for
# iterations 1 and 2.
./test_scd2_sequence.sh 1 2 customers_scd0
./test_scd2_sequence.sh 1 2 customers_scd1

# Negative test: deleted_at_column on a type 0/1 model must raise a compiler error.
./test_scd_negative.sh
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