# Integration Tests for dbt_scd2_utils

This directory contains integration tests for the dbt-scd2-utils package, organized by functionality.

## Directory Structure

### Models

#### `models/scd2_materialization/`
Tests for the core SCD2 materialization functionality:
- **`customers_scd2.sql`** - Main SCD2 materialization test model
- **`redelivered_history_scd2.sql`** - Survivor parity between the initial load and the incremental merge when a bulk reload re-delivers earlier-dated content with a later `_loaded_at`; golden seeds `redelivered_history_expected_{1,2}` are identical because iteration 2 (incremental over the same input) must be a no-op. Run `./test_scd2_sequence.sh 1 2 redelivered_history_scd2`.
- **`schema.yml`** - Tests and configuration for SCD2 models

Golden seeds compared with `matches_expected_seed` need real `TIMESTAMP_TZ` columns, so declare them with the seed `column_types` config; `columns[].data_type` alone is overridden by the inferred type on dbt Fusion (dbt1058).

#### `models/scd_materialization/`
Tests for the generic `scd` materialization (types 0 and 1):
- **`customers_scd0.sql`** - SCD type 0 model (insert only, original value retained)
- **`customers_scd1.sql`** - SCD type 1 model (one current row per key, overwritten)
- **`customers_scd1_deleted_at_invalid.sql`** - Disabled negative fixture; `deleted_at_column` on a type 0/1 model must raise a compiler error (see `test_scd_negative.sh`)
- **`schema.yml`** - Invariant tests plus `matches_expected_seed` behavioural checks

Behavioural expectations live in `seeds/scd_materialization/` as `customers_scd{0,1}_expected_{iteration}.csv`; the `matches_expected_seed` test compares the model to the seed for the current `iteration`.

#### `models/fingerprint/`, `models/fingerprint_unprotected/`, `models/fingerprint_ledger/`
Fixtures for the content fingerprint macros (`macros/fingerprint/` in the package). Every
materialization shape the fingerprint has to read is here: the package `scd` types 1 and 2, dbt
`incremental` merge and append, dbt `table`, a test-only `overwrite_table` (insert overwrite, the
project's truncate_insert shape), merges with `on_schema_change` set, a view, a table without a
loaded-at column, and a table kept at retention 0. Each parent has a child built by the test-only
`guarded_table` materialization, which asks `fingerprint_should_skip()` first.

- **`fp_ledger`** - view over this deploy's rows in the `fingerprint_deploy_node` ledger; the
  `matches_expected_seed` test compares it to `seeds/fingerprint/fp_expected_<n>.csv`, keyed by
  the `fp_iteration` var.
- **`fp_child_late`** - disabled unless `fp_enable_late_child` is set, so it can appear mid-sequence.

The `fp_edp` group (scenarios 33 to 38) holds the project's own shapes:

- **`fp_dim_versioned`** - a versioned `scd` dim built as `fp_dim_versioned_v1`; `fp_child_of_versioned`
  refs the bare name, so its `depends_on` carries the versioned unique_id and the latest-version pointer
  view Fusion may create is never fingerprinted.
- **`fp_stg_batch`** - a staging merge whose `_loaded_at` is a batch stamp, steered by `fp_batch_stage`:
  empty at snapshot, then two batches, then a late chunk of the current batch landing exactly at the
  watermark (must read `modified`), then a new batch.
- **`fp_registry`** - a registry seed with no loaded-at column, the shape of the real ones. It reads
  `unhashable` whether or not its content changed, so `fp_child_of_registry` always builds. Scenario 37
  pins that.
- **`fp_seed_live`** - a seed with `_loaded_at` whose CSV the runner rewrites in scenario 37 so a reload
  carries changed content and reads `modified`. The runner restores the committed CSVs when it exits.

The guard also refuses to skip a node whose own source checksum differs from its last fingerprinted
build of the same relation. Fixture model SQL is steered by vars, not edits, so guarded checksums never
move between scenarios except where a scenario means them to: the runner rewrites two seed CSVs and one
guarded model (`fp_edge_child_multi`) and restores them when it exits. Every group's first scenario
builds each guarded child, so the expected verdicts hold from the first run on a fresh schema and on
every rerun.

The `fp_edge` group (scenarios 39 to 53) holds the guard's remaining branches and the hooks' edge cases:

- **`fp_edge_parent_a`**, **`fp_edge_parent_b`** - two merge parents with a value flip each; `b` can also be
  made to fail, so it stays `pending` for the rest of its deploy.
- **`fp_edge_eph`** and **`fp_edge_child_of_eph`** - an ephemeral over `a` and a guarded child that reads only
  through it. The ephemeral is never registered; the guard looks through it to `a`.
- **`fp_edge_child_multi`** and **`fp_edge_grandchild`** - a two-parent child (one blocking parent builds it,
  as does its own SQL changing) and a guarded child of it, which skips only when its parent was skipped.
- **`fp_edge_adopted`** - a table first built with the fingerprint off, under an alias fresh per run: no
  baseline for the relation, so it builds once, then skips.
- **`fp_edge_scd2`** - the validity blind spot pinned: a full refresh with a different `default_valid_to` reads
  `unchanged` with the scd columns excluded (the default) and `modified` with `fingerprint_exclude_scd_columns`
  false.
- **`fp_edge_norowts`** (its folder switches `ROW_TIMESTAMP` off before the fingerprint runs) and
  **`fp_edge_numeric`** (an epoch-number loaded-at) - both `unhashable`, both children always build.
- **`fp_edge_geo`** - a `GEOGRAPHY` column left out of the hash beside an `OBJECT` and an `ARRAY` that are in it.
- **`fp_edge_quoted`** - mixed-case, space-bearing identifiers, `meta.fingerprint_loaded_at` and a per-model
  `meta.fingerprint_exclude` on a column that changes every build.
- **`fp_edge_nulls`** - rows with a null loaded-at, moved into the null bucket, added, changed and removed;
  none may read `appended`.
- **`fp_edge_ntz`** and **`fp_edge_date`** - `timestamp_ntz` under a Pacific/Auckland session and `date`
  loaded-at columns: a row one hour above the watermark reads `appended`, a row on the watermark day `modified`.

Two singular tests ride along with every group: `fp_one_pending_per_node` (registration stays idempotent
across the steps and retries of one deploy) and `fp_skipped_left_alone` (no row of a skipped node's table
was committed at or after the deploy's snapshot). Scenarios 52 and 53 fail a parent on purpose: the
children then build on its `pending` verdict, and a retry under the same deploy id settles it against the
original snapshot.

The source rows come from `fp_customer_rows()` (`macros/fp_fixture_sql.sql`), steered by vars:
`fp_source` picks a seed, `fp_upper_email` / `fp_null_email_customer` change values below the
watermark, `fp_extra_column` / `fp_drop_column` change the shape, `fp_duplicate_rows`,
`fp_bump_loaded_at` and `fp_shuffle` exercise the hash.

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

#### Content Fingerprint
```bash
# Fifty-three ordered, stateful scenarios; each builds a selection with fingerprint: true and
# asserts every fixture's verdict (new / unchanged / appended / modified / unhashable / error /
# skipped) against fp_expected_<n>. Weighted towards false negatives: genuine changes that must
# not be waved through, and guarded children that must build.
./test_fingerprint_sequence.py --profile default --target dev

# A slice, with the ledger dumped after each scenario
./test_fingerprint_sequence.py --only 9,10,12 --show

# Scenario 12 deletes fixture rows inside the build with a pre-hook (fp_delete_customer var).
```

Fixture tables and seeds get `data_retention_time_in_days = 1` from a post-hook, because the
fingerprint reads the pre-build table through Time Travel and dev databases here have retention 0.

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