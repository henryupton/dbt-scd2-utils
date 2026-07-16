# Design: `_checksum` audit column

Date: 2026-07-16

## Summary

Add an optional, opt-in `_checksum` audit column to the `scd` / `incremental_scd2`
materialization, for all SCD types (0, 1, and 2). It is a content fingerprint of the
model's business columns, computed with the same `generate_surrogate_key` (md5) function
that the envato-data-platform staging layer uses for its `_checksum` (via the
`generate_checksum` wrapper). Off by default, so existing models are unaffected.

## Motivation

The envato staging convention exposes a `_checksum` content fingerprint so downstream layers
can compare rows for content equality and, notably, feed it back into this package to
version on content rather than per-extract (`change_columns: {include: ['_checksum']}`). SCD
dimension models built with this package have no equivalent unless a staging step produced
one. An optional `_checksum` output column lets any SCD model emit the same platform-standard
fingerprint per row/version without a separate staging model.

## Configuration

Resolved from the model's `meta` block first, then a global `vars` default under
`dbt_scd2_utils`, using the existing `get_config_value` / `get_from_object` pattern.

| Option | Default | Description |
|--------|---------|-------------|
| `track_checksum` | `false` | Master switch for the `_checksum` column. |
| `checksum_column` | `_CHECKSUM` | Output column name. |

Unlike `track_previous_version` / `track_changed_columns` (SCD2 only), `track_checksum`
applies to every SCD type, so there is no type-0/1 warning.

```sql
{{
  config(
    materialized='scd',
    scd_type=1,
    unique_key=['customer_id'],
    meta={'track_checksum': true}
  )
}}
```

## Column contents

`_checksum = generate_surrogate_key(checksum_columns)` where:

- **`checksum_columns` = all source columns EXCEPT the SCD audit columns and the lifecycle
  columns** (`updated_at_column`, `created_at_column`, `deleted_at_column`). That is, the
  model's business columns including the natural key, excluding the "when was this row
  emitted / last touched" columns. This mirrors envato's rule: hash the business columns,
  never the lifecycle / audit / watermark columns.
- Columns are **sorted alphabetically** (case-insensitive) before hashing, so the checksum is
  deterministic regardless of select-list ordering (envato does this deliberately).
- The hash is the standard `dbt_utils.generate_surrogate_key` md5 hex string, identical in
  algorithm to envato's `generate_checksum`.

This is intentionally NOT the same as the package's internal `_scd2_hash` (which excludes the
key, excludes `updated_at`, is not sorted, and honours `change_columns`): `_scd2_hash` drives
version detection, `_checksum` is a stable full-content fingerprint.

Example (business columns `customer_id`, `email`, `status`; lifecycle `_updated_at`):

```
_checksum = md5( customer_id | email | status )   -- alphabetical, one 32-char hex string
```

## Value derivation and immutability

`_checksum` is a pure function of a row's own business columns (no window functions, no
dependence on neighbouring versions). Its behaviour on write differs by type:

- **Type 0** (immutable, one row per key): computed on insert, never changes.
- **Type 1** (overwrite in place): computed on insert AND recomputed on the matched
  `update set`. Type 1 overwrites business columns in place, so the checksum must be
  refreshed to match, otherwise it goes stale. This is the one non-obvious per-type detail.
- **Type 2** (versioned history): computed per version on insert. A version's business
  columns are immutable, so `_checksum` is NOT added to `merge_update_cols` (unlike
  `_previous` / `_changed`, which shift with neighbours). `redundant_versions` carries a
  `cast(null as varchar)` placeholder so the `union all` stays aligned; those rows are
  deleted by the MERGE, so the value is irrelevant.

## Components and changes

All paths are under the SCD framework (`macros/materializations/scd/`).

### New macro

`macros/materializations/scd/columns/get_checksum_sql.sql`:
`get_checksum_sql(checksum_columns)` returns `{{ dbt_utils.generate_surrogate_key(checksum_columns) }}`.
A thin, intent-named wrapper (mirrors the other `columns/` macros and envato's
`generate_checksum`). The caller passes the already-sorted `checksum_columns`.

### `scd_plan.sql` (shared, applies to all types)

- Resolve `track_checksum` and `checksum_column`.
- After `audit_columns` and `dest_columns` are known, compute
  `checksum_columns = sorted(dest_columns - audit_columns - {updated_at, created_at, deleted_at})`
  (case-insensitive difference, uppercased names, `| sort`).
- When `track_checksum`, append `checksum_column` to `audit_columns` here (shared section,
  before the type-0/1 return), so all three types include it in their column lists. Order:
  after `_change_type`, before the type-2-only `_previous` / `_changed`.
- Thread `track_checksum`, `checksum_column`, and `checksum_columns` into both the type-0/1
  arg_dict and the type-2 `default_arg_dict`.

### Type 2 builders

- `types/type_2/get_initial_load_scd2_sql.sql`: in the final select, emit `_checksum` after
  `_change_type` and before the existing `_previous` / `_changed` conditionals.
- `types/type_2/get_incremental_scd2_sql.sql`: same position in `scd2_versions`; add a
  `cast(null as varchar)` placeholder in `redundant_versions` at the matching position. Not
  added to `merge_update_cols`.

### Type 0 builders

- `types/type_0/get_initial_load_scd0_sql.sql` and `get_incremental_scd0_sql.sql`: emit
  `_checksum` after `_change_type` in the select. Insert-only, so no update handling.

### Type 1 builders

- `types/type_1/get_initial_load_scd1_sql.sql`: emit `_checksum` after `_change_type`.
- `types/type_1/get_incremental_scd1_sql.sql`: emit `_checksum` in the source select AND add
  `DBT_INTERNAL_DEST.<checksum_column> = DBT_INTERNAL_SOURCE.<checksum_column>` to the matched
  `update set`, so an overwritten row's checksum is recomputed.

## Edge cases

- First-seen / only row: `_checksum` is always populated (it is content, not history), so it
  is never NULL on a live row.
- Empty `checksum_columns` (every column is audit or lifecycle): not expected in practice
  because the natural key is always included. Not special-cased.
- Non-scalar columns (ARRAY / OBJECT / VARIANT / GEOGRAPHY): envato hand-casts these
  (`::variant`) or omits GEOGRAPHY, because `cast(... as varchar)` is rejected or
  non-deterministic. This package derives `checksum_columns` automatically and does not cast
  them specially, so a model with such a column in scope may error or hash
  non-deterministically. Documented as a limitation; such models should exclude or omit those
  columns (out of scope for this change).

## Testing

Add integration coverage across all three types, with `track_checksum` enabled:

- Type 2: `_checksum` present and equal for two versions with identical business columns,
  different for a version whose business columns changed; stable across a full refresh vs
  incremental.
- Type 1: `_checksum` recomputed when a key's business columns are overwritten (not stale).
- Type 0: `_checksum` reflects the original (retained) row.
- A determinism check: `_checksum` is independent of select-list column order (two models
  with the same business columns in different order produce the same checksum).

Assertions use singular tests (SQL returning offending rows) and, where practical,
`generate_surrogate_key` recomputed in the test as an oracle.

## Documentation

Add a "Content Checksum" subsection to the README (near the previous/changed section),
covering the option, the all-types applicability, the business-columns / alphabetical
definition, parity with the staging `_checksum`, and the non-scalar-column limitation. Add
the two options to the configuration table.

## Out of scope (YAGNI)

- No `generate_checksum` alias macro or staging-style call-site enumeration; the package
  derives the column set automatically.
- No special casting for non-scalar columns (documented limitation).
- `_checksum` is not wired into change detection (it is an output only; users version on
  content via `change_columns` today).
