# Integration Tests for dbt_scd2_utils

This directory contains integration tests for the SCD Type 2 incremental materialization.

## Test Structure

### Seeds (Test Data)
- `customers_raw.csv` - Initial customer data (3 records)
- `customers_update1.csv` - First update batch (email change, status change, new customer)
- `customers_update2.csv` - Second update batch (status change, email change)
- `expected_initial_load.csv` - Expected output after initial load
- `expected_after_update1.csv` - Expected output after first incremental run

### Test Models
- `test_scd2_basic.sql` - Tests SCD2 with default column names
- `test_scd2_custom_columns.sql` - Tests SCD2 with custom column configurations

### Tests
- `test_scd2_behavior.sql` - Validates correct number of current records
- `test_unique_current_records.sql` - Ensures each customer has only one current record
- `test_valid_date_logic.sql` - Validates date logic (valid_from <= valid_to)

## Running Tests

1. Install dependencies:
   ```bash
   dbt deps
   ```

2. Run seeds to load test data:
   ```bash
   dbt seed
   ```

3. Run models (initial load):
   ```bash
   dbt run
   ```

4. Run tests:
   ```bash
   dbt test
   ```

5. For incremental testing, update seed references in models and re-run:
   ```bash
   # Update model to reference customers_update1
   dbt run
   dbt test
   ```

## Test Scenarios

1. **Initial Load**: Verify SCD2 table creation with proper audit columns
2. **Incremental Updates**: Test that changed records create new versions and expire old ones
3. **Unchanged Records**: Verify unchanged records remain untouched
4. **New Records**: Test insertion of completely new records
5. **Custom Configurations**: Validate custom column name configurations work properly