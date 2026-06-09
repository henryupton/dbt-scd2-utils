# Born-deleted SCD2 records — design

**Date:** 2026-06-09
**Base:** `origin/main` (`cfcdd86`) — the generic `scd` materialization (types 0/1/2, PR #6)
**Branch:** `worktree-fix+scd2-born-deleted-records`
**Status:** Semantics approved; root cause to be confirmed by reproduction

## Problem

When a key's **first and only** source row arrives already soft-deleted —
because ingestion started *after* the soft delete happened — the SCD2
materialization mis-records it. The only state we ever hold for the entity is
the deleted one, and it should be represented as deleted.

Observed on `recast.dw_envato.dim_author` (e.g. `author_id =
'af336fce-2ad7-4a80-bf63-50e78a91788f'`) and reported on the meta-config branch.

## Desired behaviour

For a born-deleted record (first/only version has `deleted_at IS NOT NULL`):

| column | value | rationale |
|---|---|---|
| `_change_type` | `'D'` | the only state we hold is the deleted state |
| `_valid_from` | `deleted_at` | the deleted state began at deletion time, not at `created_at` |
| `_valid_to` | default (`2999-12-31 …`) | open — it is the latest state |
| `_is_current` | `true` | `is_current` reflects the entity's current state; the current state is "deleted" |

Consistent with how the package already treats a *terminal* delete (latest
version is a delete): `is_current = true`, open `valid_to`.

## Reconciliation with the code on this base

Static reading of `origin/main`:
- `get_change_type_sql` already returns `'D'` first when `deleted_at IS NOT NULL`,
  and `deleted_at_column` is plumbed correctly through `scd_plan` to both the
  initial-load and incremental type-2 macros. So **`_change_type` should already
  resolve to `'D'`** here when the model configures `deleted_at_column`.
- `get_valid_from_sql` has **no** `deleted_at` handling. A born-deleted first
  record therefore takes the `created_at` override (or `updated_at`), never
  `deleted_at`. This is the confirmed bug and it also violates the existing
  `no_records_after_deletion` test (which asserts every deleted record has
  `valid_from = deleted_at`).
- `customers_scd2` sets `_created_at = _updated_at`, so the existing suite cannot
  surface this — the reproduction needs `created_at`, `deleted_at`, `updated_at`
  all distinct.

Because the reported `'I'` is not explained by a static read of this base, the
work **reproduces first** and fixes what the evidence shows. The reproduction
asserts both `_change_type = 'D'` and `_valid_from = deleted_at`, so whichever is
actually broken is caught. The `valid_from` fix is needed regardless.

## Existing invariant this aligns with

`no_records_after_deletion` already asserts that any record with
`deleted_at IS NOT NULL` has `valid_from = deleted_at`. Nothing breaks in the
current fixtures only because their delete events have `updated_at == deleted_at`
(and `created_at == updated_at`), so `valid_from` lands on `deleted_at` by
coincidence. A born-deleted first record with distinct timestamps breaks that.

## Changes (paths are `origin/main` layout)

### Materialization logic
- **`macros/materializations/scd/columns/get_valid_from_sql.sql`** — add a
  `deleted_at_col` parameter. For the **first record** of a key, set
  `valid_from = coalesce(deleted_at, created_at, updated_at)` (deleted_at wins
  when present; else existing created_at/updated_at logic). Non-first records
  unchanged (`updated_at`), to preserve validity-window continuity — the prior
  version's `valid_to` is driven by `lead(updated_at)`.
- **`macros/materializations/scd/types/type_2/get_initial_load_scd2_sql.sql`**
  and **`.../get_incremental_scd2_sql.sql`** — pass `deleted_at_col` into the
  `get_valid_from_sql(...)` call.
- **`macros/materializations/scd/columns/get_change_type_sql.sql`** — verify it
  emits `'D'` for the born-deleted case; change only if the reproduction shows
  otherwise.

### Generic tests
- **Replace `first_record_insert` with `first_record_not_update`.** The real
  invariant is that a key's first record cannot be an update — insert *or* delete
  are both valid firsts. Rename `tests/generic/test_first_record_insert.sql` →
  `test_first_record_not_update.sql`, rename the test macro, change the predicate
  from `_change_type != 'I'` to `_change_type = 'U'`, update the docstring.
  Breaking change to the package's public test surface → version bump on release.

### Reproduction fixture (mirrors the `customers_scd2` pattern)
- `integration_tests/seeds/scd2_materialization/born_deleted_raw_1.csv`,
  `born_deleted_raw_2.csv`; add both to
  `integration_tests/seeds/scd2_materialization/schema.yml`.
- `integration_tests/models/scd2_materialization/born_deleted_scd2.sql`
  (`materialized='incremental_scd2'`, `unique_key`, `meta.deleted_at_column`,
  `meta.created_at_column`; `created_at`/`deleted_at`/`updated_at` **distinct**
  so the `valid_from` bug manifests).
- Add a `born_deleted_scd2` section to
  `integration_tests/models/scd2_materialization/schema.yml` wiring
  `first_record_not_update`, `no_records_after_deletion`, `latest_row_is_current`,
  `one_current_per_key`, `no_validity_overlaps`, plus a singular assertion that
  the born-deleted key has `_change_type = 'D'`, `_valid_from = deleted_at`,
  `_is_current = true`.

Two iterations to cover **both** code paths (different macros):
- **Iteration 1 (full refresh → `get_initial_load_scd2_sql`):** a born-deleted
  key present at initial load.
- **Iteration 2 (incremental → `get_incremental_scd2_sql`):** a *second*,
  brand-new born-deleted key arrives while the table already exists.

The model clamps its iteration so it parses for any iteration the shared
sequence runner uses.

### Schema / docs reference updates
- `integration_tests/models/scd2_materialization/schema.yml` — swap
  `first_record_insert` → `first_record_not_update` on `customers_scd2`.
- `README.md` — update the "Available Tests" entry for the renamed test.

## Test strategy (TDD)

1. Add the fixture and assertions; run `--profile default --target dev` and
   capture what's actually wrong (red).
2. Diagnose from compiled SQL / output.
3. Apply the minimal fix (`valid_from`, and `change_type` only if shown broken).
4. Re-run the born-deleted suite **and** the existing `customers_scd2` suite —
   all green, no regressions.

## Out of scope

- Changing the semantics of *normal* (mid-timeline) deletes.
- Treating a deleted entity as `is_current = false`.
- Synthesising the entity's pre-deletion (alive) history we never ingested.
- The `collapse_redundant_versions` / out-of-order work (lives on the meta-config
  branch; not on this base).
