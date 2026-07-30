# Design: non-null key matching and optional Search Optimization

Date: 2026-07-18

## Summary

Two linked changes to the SCD Type 2 incremental path:

1. **Non-null key matching (default).** The incremental MERGE `ON` and the `previous_record`
   lookup currently match the key with `equal_null` (null-safe). `equal_null` /
   `IS NOT DISTINCT FROM` disables Snowflake's Search Optimization Service and blunts
   micro-partition pruning. A business key should be non-null, so the default becomes plain
   `=`, which is prune-friendly and Search-Optimization-eligible. Correctness for the
   nullable-key case is preserved, not dropped: it is inferred, guarded, or opted into.

2. **Optional Search Optimization Service (opt-in).** A new `search_optimization` config
   issues `ALTER TABLE <target> ADD SEARCH OPTIMIZATION ON EQUALITY(<key columns>)` so the
   package can manage the search access path the custom materialization otherwise bypasses.
   Off by default (Enterprise Edition, ongoing cost).

Change 1 is the substance and is solid on its own. Change 2 depends on change 1 to be
useful and carries an explicit "verify with the query plan" caveat (see Open validation).

## Dependency

Builds directly on PR #13 (`perf/scd2-incremental-efficiency`), which replaced the
`generate_surrogate_key` (md5) key match with raw-column `equal_null` in both the
`previous_record` lookup and the MERGE `ON`, specifically to enable pruning. This design
assumes that raw-column matching is in place. Implementation should land after #13 merges.

## Motivation

PR #13 made the key match prune-friendly by matching raw columns instead of a hash, but it
used `equal_null` for null safety. Two consequences:

- **Search Optimization can't fire.** SOS supports equality (`=`) and `IN`, including
  `probe.col = build.col` joins, but explicitly not `EQUAL_NULL` / `IS NOT DISTINCT FROM`
  (see Sources). So an SOS-on-the-target feature does nothing for the merge while the match
  is `equal_null`.
- **Pruning is weaker.** `equal_null` is a poorer signal for partition pruning than `=`.

`equal_null` is only needed when a key column can actually be NULL. A unique/business key
should not be nullable (uniqueness and NULL are in tension, and a Snowflake `NOT NULL` key is
a hard guarantee). So the safe, fast default is `=`, with `equal_null` reserved for the
genuinely-nullable case, chosen automatically wherever possible.

## Configuration

Resolved from the model's `meta` block first, then a global `vars` default under
`dbt_scd2_utils`, via the existing `get_config_value` / `get_from_object` pattern.

| Option | Default | Description |
|--------|---------|-------------|
| `assume_keys_not_null` | `true` | `true` → match keys with `=` (prune/SOS-eligible). `false` → match with `equal_null` (null-safe, not SOS-eligible). Unset resolves via the inference ladder below. |
| `search_optimization` | `false` | `true` → add SOS `EQUALITY` on the key columns. On/off flag. |
| `search_optimization_columns` | `unique_key` | Columns to build `search_optimization` on. Ignored when the flag is off. |

`assume_keys_not_null` is a three-state control (explicit `true`, explicit `false`, unset),
because unset triggers inference:

- **unset (default):** infer from declared constraints, else use `=` protected by the null
  guard (below).
- **`true`:** force `=`, skip the guard (caller asserts clean keys; cheapest).
- **`false`:** force `equal_null`, skip the guard and its warning (caller knows keys are
  nullable).

```sql
{{
  config(
    materialized='incremental_scd2',
    unique_key=['customer_id', 'region'],
    meta={'assume_keys_not_null': false}   -- region is genuinely nullable
  )
}}
```

## Operator selection (change 1)

The operator (`=` vs `equal_null`) used in the `previous_record` `where` and the MERGE `ON`
is chosen per run by this ladder, cheapest first:

1. **Explicit config.** `assume_keys_not_null` set → honour it (`true` → `=`, `false` →
   `equal_null`). Done.
2. **Declared constraints.** Unset, and every `unique_key` column carries a `not_null`
   constraint on the model node (`model.columns[col].constraints`, from schema.yml /
   contract) → `=`, no guard. Snowflake enforces `NOT NULL` (the only enforced constraint;
   `PRIMARY KEY` / `UNIQUE` / `FOREIGN KEY` are informational), so a declared-and-enforced
   `NOT NULL` is trustworthy.
3. **Null guard.** Unset and not provably non-null → default to `=`, but run a cheap check on
   the delta temp table:

   ```sql
   -- one count_if per unique_key column, summed
   select count_if(<key_col_1> is null) + count_if(<key_col_2> is null) + ... as n_null
   from {{ tmp_relation }}
   ```

   fetched into Jinja. If any key column has a null, **warn and fall back to `equal_null`**
   for this run. Otherwise use `=`.

The guard runs against the small delta temp (already materialised before the merge SQL is
generated in `scd_plan`), so it is negligible next to the merge and only runs on tables
without an explicit setting or declared constraints.

### Why the default is safe and non-breaking

With warn-and-fall-back, no model's output changes:

- **Clean keys (the norm):** with no nulls, `=` and `equal_null` are identical, so output is
  unchanged and the model becomes prune/SOS-eligible.
- **Nullable keys:** the guard trips, falls back to `equal_null`, and output is identical to
  today plus a warning naming the offending column(s).

So this ships as a **non-breaking minor**, not a major version. The only new cost is one
guard query on unconstrained, unset tables. (Reserve a major bump only if a future decision
drops the fallback and hard-errors instead.)

## Search Optimization Service (change 2)

Opt-in via `search_optimization`. When enabled, the materialization manages the search access
path itself, because the custom `scd` / `incremental_scd2` materialization does not go
through dbt's built-in table materialization and so never honours native SOS handling.

- **Target columns:** `search_optimization_columns`, defaulting to the `unique_key` columns
  (the merge join / lookup keys). `EQUALITY` method only.
- **Statement:** `alter table <target> add search optimization on equality(<cols>)`.
- **Lifecycle / idempotency:** SOS is a persistent table property. `create or replace` on a
  full refresh drops it, so it is re-added on the create; on an incremental run it persists and
  must not be re-added (re-adding an existing path errors). Resolved by gating on the
  full-refresh / create path only (`so_columns` is threaded through the plan as `none` on
  incremental runs), which needs no state lookup. Enabling SOS on an existing model therefore
  takes effect on the next `--full-refresh`.
- **Guard against a useless enable:** if `search_optimization` is on but the resolved key
  operator is `equal_null` (nullable keys), warn that SOS will not accelerate the merge while
  the match is null-safe.
- **Edition / cost:** Enterprise Edition only; ongoing storage + serverless maintenance cost
  proportional to table size and churn. Documented loudly; off by default.

### Open validation (must precede claiming a merge speedup)

SOS's benefit to a *batch* MERGE is unproven and plausibly weaker than clustering's range
pruning (SOS shines on selective point lookups, not wide batches). Before documenting any
merge speedup, use the repo's `analyse-query-plan` skill to `EXPLAIN` a real incremental
merge with and without SOS and confirm the access path actually changes. SOS's benefit to
downstream consumer point-lookups (`where customer_id = ... and _is_current`) is solid
regardless; if the merge benefit doesn't materialise, `search_optimization` still stands as a
consumer-query accelerator and should be documented as such.

## Components and changes

All under `macros/materializations/scd/`.

### `scd_plan.sql`

- Resolve `assume_keys_not_null` (config → var), `search_optimization`, and
  `search_optimization_columns` (default `unique_key`).
- Run the operator-selection ladder after `build_temp_table` (temp exists) and before the
  merge SQL is generated. Emit the `not_null` inference from `model.columns[...].constraints`
  and, when needed, the fetched null-guard result.
- Thread the resolved boolean (call it `key_match_null_safe`) into the type-2 `arg_dict`.
- After the main statement (table built), when `search_optimization` is set, run the
  add-SOS-if-missing logic (a new macro), and the useless-enable warning.

### `types/type_2/get_incremental_scd2_sql.sql`

- Replace the hard-coded `equal_null(...)` in both the `previous_record` `where` and the
  MERGE `ON` with a helper that emits `=` or `equal_null` per `key_match_null_safe`.
  Everything else (the `_scd2_key` dedup, `_scd2_hash` run detection, `is distinct from`
  no-op update guard from #13) is unchanged.

### New macros

- `get_key_match_sql(columns, left_alias, right_alias, null_safe)`: returns the `and`-joined
  per-column predicate, `equal_null(l.c, r.c)` when `null_safe` else `l.c = r.c` (aliases passed
  without the dot, which the macro adds). Used by both
  the lookup `where` and the MERGE `ON` so they can never drift apart (a correctness invariant
  the null_key regression suite depends on).
- `add_search_optimization(relation, columns)`: reads current SOS state and issues
  `add search optimization on equality(...)` for any missing columns; no-op otherwise.

## Edge cases

- **`updated_at` in the version key.** The MERGE matches on `scd2_unique_key`
  (`unique_key + updated_at`). `updated_at` is the ordering column and assumed non-null; the
  guard checks the business `unique_key` columns. The operator applies uniformly to all key
  columns in the predicate.
- **Mixed nullability across key columns.** Inference requires *all* key columns declared
  `not_null`; any undeclared column drops to the guard. The guard checks all key columns; a
  null in any one triggers fallback.
- **Full refresh.** The initial load is a CTAS with no MERGE, so operator selection does not
  apply there; SOS re-add does (the create dropped it).
- **Contract without enforcement.** A declared `not_null` constraint on a non-enforced
  contract is a modelling assertion, not a Snowflake guarantee. Treated as sufficient for `=`
  (consistent with trusting declared constraints); the guard is the safety net for anyone who
  neither declares nor enforces.

## Testing

- **Non-null keys, default path:** existing scd2 golden suites must pass unchanged with `=`
  (proves output-identical for clean data).
- **Nullable keys, guard fallback:** the `null_key` fixture (nulls in a composite key) with
  `assume_keys_not_null` unset must warn, fall back to `equal_null`, and still match the
  existing `null_key_expected_*` golden snapshots from #13.
- **Nullable keys, explicit `false`:** same golden snapshots, no guard query, no warning.
- **Declared `not_null`:** a model with `not_null` constraints on the key uses `=` with no
  guard query (assert via the compiled SQL / absence of the guard statement).
- **SOS lifecycle:** `search_optimization` on → SOS present after initial load; still present
  (not re-added / no error) after an incremental run; re-added after `--full-refresh`.
- **Useless-enable warning:** `search_optimization` on with `assume_keys_not_null: false`
  warns.

## Documentation

- README configuration table: add `assume_keys_not_null` and `search_optimization`.
- New "Key matching and pruning" subsection: the non-null default, the inference ladder, the
  guard/warning, and the nullable-key opt-out.
- New "Search Optimization" subsection: opt-in, target columns, Enterprise/cost caveat, and
  the honest merge-vs-consumer benefit framing pending query-plan validation.

## Out of scope (YAGNI)

- **Clustering keys** (`cluster_by` / automatic clustering) — the stronger lever for batch
  merge pruning. Its own spec.
- **Auto incremental predicates** (validity watermark to bound the target scan) — its own
  spec.
- **SOS methods beyond `EQUALITY`** (`SUBSTRING`, `FULL_TEXT`, geo) — not relevant to key
  matching.
- **Hard-error guard variant** — deferred; the fallback keeps this non-breaking. Revisit if a
  strict mode is wanted (that would be the 2.0).

## Sources

- Search optimization, point lookups: https://docs.snowflake.com/en/user-guide/search-optimization/point-lookup-queries
- Search optimization, joins: https://docs.snowflake.com/en/user-guide/search-optimization/join-queries
