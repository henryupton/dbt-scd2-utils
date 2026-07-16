# Design: `_previous` and `_changed` audit columns

Date: 2026-07-16

## Summary

Add two independent, opt-in audit columns to the SCD **type 2** path of the `scd` /
`incremental_scd2` materialization:

1. `_previous`: an `OBJECT` holding the tracked change columns of the immediately
   preceding version of the entity. Lets a consumer diff any tracked field against its
   prior value.
2. `_changed`: an `OBJECT` of booleans, one key per tracked change column, `true` when
   that column changed since the prior version. A precomputed change map derived from the
   same comparison.

Both are off by default so existing models are unaffected. Both are derived from the same
versioned timeline (`lag` over the business key ordered by the updated-at column) and are
populated in both the initial load and incremental (MERGE) code paths.

## Scope: SCD type 2 only

The package now ships a generic `scd` materialization supporting types 0, 1, and 2 (with
`incremental_scd2` retained as a type-2 alias). "Previous version" and "what changed since
the prior version" only have meaning where history exists, so these columns apply to
**type 2 only**. Setting either switch on a type 0 or type 1 model emits a warning at plan
time and the columns are simply not produced (the model builds normally without them). A
warning rather than a hard error keeps a folder-wide `+meta` switch from breaking a layer
that happens to include a type 0/1 model.

## Motivation

SCD2 tables record that a version changed, but not what changed. Consumers currently have
to self-join a row against its predecessor to work that out. Materialising the prior
payload (`_previous`) and a per-column change map (`_changed`) removes that join and makes
"what fields moved between these two versions" a single-column lookup.

## Configuration

Both columns follow the package's existing option-resolution style: read from the model's
`meta` block first, then fall back to a global `vars` default under `dbt_scd2_utils`.

| Option | Default | Description |
|--------|---------|-------------|
| `track_previous_version` | `false` | Master switch for the `_previous` column. |
| `previous_version_column` | `_PREVIOUS` | Output column name for the prior-version object. |
| `track_changed_columns` | `false` | Master switch for the `_changed` column. |
| `changed_columns_column` | `_CHANGED` | Output column name for the change-map object. |

Per-model example:

```sql
{{
  config(
    materialized='incremental_scd2',
    unique_key=['customer_id'],
    meta={
      'track_previous_version': true,
      'track_changed_columns': true,
      'change_columns': {'exclude': ['_written_at']}
    }
  )
}}
```

### Enabling per layer, not globally

These columns are intended for selective use (for example the staging layer, not
dimension tables), so enablement is per table by design. The recommended pattern is to
flip a whole folder on with `+meta` in `dbt_project.yml`:

```yaml
models:
  my_project:
    staging:
      +meta:
        track_previous_version: true
        track_changed_columns: true
    marts:
      # nothing set, so dimension tables stay off
```

The `vars` defaults for the two switches stay `false` and are not intended to be flipped
globally. Only the column-name options are worth setting via `vars` (to rename the output
columns consistently). A single model can still override with its own `meta` block.

## Column contents

Both objects are built over the **tracked change columns** only, that is the
`scd_check_columns` set the plan already computes (respecting `change_columns` include and
exclude, and the legacy `scd_check_columns` and `exclude_columns_from_change_check`). This
keeps the objects aligned with exactly the columns whose changes trigger a new version.

### `_previous`

For each version, an `OBJECT` containing the tracked columns of the immediately preceding
version in that key's timeline. The first version of a key has no predecessor, so its
`_previous` is `NULL`.

Built with `object_construct_keep_null` so a column that was genuinely `NULL` in the prior
version still appears as a key with a null value.

```
-- first version
_previous = null

-- second version (email changed)
_previous = { "email": "john@old.com", "status": "active" }
```

### `_changed`

For each version, an `OBJECT` with one key per tracked column, value `true` when that
column's value differs from the prior version and `false` otherwise. Comparison uses
`IS DISTINCT FROM`, so a null-to-value or value-to-null transition counts as changed and
null-to-null does not. The whole object is `NULL` for a key's first version, matching
`_previous`.

```
-- first version
_changed = null

-- second version (email changed, status did not)
_changed = { "email": true, "status": false }
```

### Object key casing and its limitation

Object keys are lowercased (`"email"`, not `"EMAIL"`), so downstream reads are not shouted
at in all-caps.

Limitation: Snowflake object path access is case-sensitive. Because keys are stored
lowercase, consumers must reference them in lowercase (`_previous:email`, `_changed:email`)
even though the underlying table columns are uppercase identifiers. A source column whose
name only differs by case from another would collide once lowercased; this is not expected
in practice but is documented as a known limitation.

## Components and changes

All paths are under the refactored SCD framework introduced on main.

### New macros

Two expression-builder macros under `macros/materializations/scd/columns/`, mirroring the
existing `get_is_current_sql`, `get_valid_from_sql`, and `get_change_type_sql` macros.

`get_previous_version_sql(scd_check_columns, unique_keys_csv, updated_at_col)` returns:

```sql
lag(object_construct_keep_null('col_a', col_a, 'col_b', col_b, ...))
  over (partition by <unique_keys_csv> order by <updated_at_col>)
```

`get_changed_columns_sql(scd_check_columns, unique_keys_csv, updated_at_col)` returns:

```sql
case
  when lag(<updated_at_col>) over (partition by <unique_keys_csv> order by <updated_at_col>) is null
    then cast(null as object)
  else object_construct_keep_null(
    'col_a', (col_a is distinct from lag(col_a) over (partition by <unique_keys_csv> order by <updated_at_col>)),
    ...
  )
end
```

The `lag(updated_at) is null` test identifies a key's first version (no prior row in the
window) and yields a null object there. Both macros lowercase the object keys and reference
the columns unquoted, consistent with the rest of the package.

### `scd_plan.sql`

Planning is centralised here (both the `scd` and `incremental_scd2` materializations call
it). Changes:

- Resolve `track_previous_version`, `previous_version_column`, `track_changed_columns`, and
  `changed_columns_column` via `get_config_value` / `get_from_object`.
- Alongside the `deleted_at_column` type-0/1 guard: if either switch is true and `scd_type`
  is 0 or 1, emit a warning (`exceptions.warn`) and do not produce the columns. A warning
  rather than a hard error keeps a folder-wide `+meta` switch from breaking a layer that
  includes a type 0/1 model.
- In the type-2 section only: when a switch is on, append its column name to
  `audit_columns` (fixed order: previous, then changed) so it becomes part of
  `all_cols_names` and the MERGE insert list; and when the switch is on **and**
  `update_all_previous_records` is true, also append it to `merge_update_cols` so a version
  shifted by a backfill or collapse gets its object recomputed. Thread the two flags and
  two column names into `default_arg_dict`.

### `types/type_2/get_initial_load_scd2_sql.sql`

In the final select over `changes_only`, conditionally append the `_previous` and
`_changed` expressions (previous first) after the existing audit columns. `CREATE TABLE AS`
infers the object column types.

### `types/type_2/get_incremental_scd2_sql.sql`

- In `scd2_versions`, conditionally append the `_previous` and `_changed` expressions after
  `_change_type` and before `'upsert' as _scd2_op` (which is followed by the trailing
  `_scd2_key`).
- In `redundant_versions`, append matching `cast(null as object)` placeholders in the same
  position, so the `union all` stays column-aligned. These rows are deleted by the MERGE,
  so the placeholder value is irrelevant.

## Data flow and correctness

Both columns are a `lag` over the canonical, post-collapse version timeline, computed in
the same CTE that already produces the SCD2 audit columns.

- **Initial load:** `lag` runs over the full source timeline, so every version gets the
  correct prior payload and change map.
- **Chronological incremental:** a new record's predecessor is loaded into
  `previous_record`, so the new row gets the correct object. Existing rows are not rewritten
  (the columns are only in `merge_update_cols` when recompute is needed).
- **Backfill, out-of-order, or collapse with `update_all_previous_records=true`:** the full
  per-key timeline is reconstructed, the objects are recomputed for every in-scope version,
  and the columns are in `merge_update_cols`, so any version whose predecessor shifted is
  corrected in place.
- **`update_all_previous_records=false`:** shifted existing rows are not recomputed, so
  their objects can go stale after a backfill. This is the same documented caveat that
  already applies to `_change_type` under this setting.

## Edge cases

- First version of a key: both objects are `NULL`.
- Empty `scd_check_columns` (nothing tracked): `_previous` and `_changed` become empty
  objects on non-first versions. Unlikely; documented, not special-cased.
- A tracked column null in the prior version: appears in `_previous` with a null value; its
  `_changed` flag reflects `IS DISTINCT FROM`.
- Switch set on a type 0/1 model: warning at plan time; the columns are not produced.

## Testing

Add an iteration-driven integration model (in the `incremental_scd2` style used by
`customers_scd2` / `ooo_backfill_scd2`), with both switches enabled, plus seed data across
two iterations. Cover:

- A key's first version has `_previous` and `_changed` both null.
- A subsequent version carries the prior tracked columns in `_previous` and the correct
  per-column booleans in `_changed`.
- A backfilled (out-of-order) arrival, run with `update_all_previous_records=true`, leaves
  the following version's `_previous` and `_changed` recomputed to point at the backfilled
  record.

Assertions use singular tests (SQL returning offending rows) that self-check the stored
objects against a `lag` oracle over the final table, so they hold on every iteration.

## Documentation

Add a "Previous Version and Change Tracking" section to the README covering both columns,
their config options, SCD2-only applicability, the lowercase-key and case-sensitivity
limitation, and a worked example. Add the four options to the configuration table.

## Out of scope (YAGNI)

- No recursive nesting: the objects never contain their own `_previous` or `_changed`.
- No prior-version audit metadata (valid_from, valid_to) in `_previous`.
- No separate "changed field names" array; `_changed` as a boolean map covers the need.
- SCD types 0 and 1: not supported (warned, columns omitted).
