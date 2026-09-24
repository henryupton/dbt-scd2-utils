#!/usr/bin/env python3
"""Content fingerprint scenario suite.

Drives the fingerprint fixtures (models/fingerprint*, seeds/fingerprint) through ordered
scenarios. After each, the matches_expected_seed test on fp_ledger asserts the verdict recorded
for every selected fixture against seeds/fingerprint/fp_expected_<n>.csv. Each scenario is its
own deploy id, so the ledger accumulates and reruns never collide. Scenarios are stateful and
run in order; --only / --from re-run a slice against whatever state the tables are in.

The suite is weighted towards false negatives: every way a genuine change could be waved
through as unchanged or appended, and every way a guarded child could be skipped when it must
build. Scenario 12 deletes fixture rows inside the build with a pre-hook (fp_delete_customer);
a change made before the build's snapshot belongs to no deploy and is correctly invisible.

Usage:
    ./test_fingerprint_sequence.py [--profile default] [--target dev] [--dbt /path/to/dbt]
                                   [--only 1,2,3] [--from 15] [--show] [--no-seed]
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import subprocess
import sys
from pathlib import Path

CORE = "tag:fingerprint"
OVER = "tag:fp_overwrite"
EVOLVE = "tag:fp_evolve"
GUARD = "tag:fp_guard"

# Fixture state after scenario 14, reused by the later groups so they start from a known shape.
STEADY = {"fp_source": "deleted_b", "fp_upper_email": True, "fp_extra_column": True, "fp_null_email_customer": 3}

SCENARIOS = [
    # --- core: initial load and the plain shapes ---------------------------------------------
    dict(id=1, desc="initial load, create or replace everywhere", select=CORE, flags=["--full-refresh"],
         vars={"fp_source": "base", "fp_full_refresh_strategy": "replace", "fp_allow_stg_full_refresh": True}),
    dict(id=2, desc="same data, incremental", select=CORE, vars={"fp_source": "base"}),
    dict(id=3, desc="two rows appended", select=CORE, vars={"fp_source": "appended"}),
    dict(id=4, desc="later version of an old key", select=CORE, vars={"fp_source": "versioned"}),
    dict(id=5, desc="logic change (upper email), full refresh", select=CORE, flags=["--full-refresh"],
         vars={"fp_source": "versioned", "fp_upper_email": True}),
    dict(id=6, desc="full refresh, nothing changed", select=CORE, flags=["--full-refresh"],
         vars={"fp_source": "versioned", "fp_upper_email": True}),
    dict(id=7, desc="column added, full refresh", select=CORE, flags=["--full-refresh"],
         vars={"fp_source": "versioned", "fp_upper_email": True, "fp_extra_column": True}),
    dict(id=8, desc="same again, incremental", select=CORE,
         vars={"fp_source": "versioned", "fp_upper_email": True, "fp_extra_column": True}),
    # --- genuine changes that must not be waved through ----------------------------------------
    dict(id=9, desc="in-place value change, same timestamps (merge rewrites the row)", select=CORE,
         vars={"fp_source": "inplace", "fp_upper_email": True, "fp_extra_column": True}),
    dict(id=10, desc="backdated row inserted below the watermark", select=CORE,
         vars={"fp_source": "backdated", "fp_upper_email": True, "fp_extra_column": True}),
    dict(id=11, desc="row removed at source, full refresh", select=CORE, flags=["--full-refresh"],
         vars={"fp_source": "deleted", "fp_upper_email": True, "fp_extra_column": True}),
    dict(id=12, desc="row deleted by a pre-hook in a month the build never touches (append and merge)", select=CORE,
         vars={"fp_source": "deleted_b", "fp_upper_email": True, "fp_extra_column": True, "fp_delete_customer": 4}),
    dict(id=13, desc="value flipped to null for one old row, full refresh", select=CORE, flags=["--full-refresh"],
         vars=STEADY),
    dict(id=14, desc="only excluded columns change (sysdate), full refresh", select=CORE, flags=["--full-refresh"],
         vars=STEADY),
    # --- insert overwrite shape ----------------------------------------------------------------
    dict(id=15, desc="overwrite: initial", select=OVER, flags=["--full-refresh"], vars=STEADY),
    dict(id=16, desc="overwrite: same rows", select=OVER, vars=STEADY),
    dict(id=17, desc="overwrite: same rows, random order", select=OVER, vars={**STEADY, "fp_shuffle": True}),
    dict(id=18, desc="overwrite: every row duplicated", select=OVER, vars={**STEADY, "fp_duplicate_rows": True}),
    dict(id=19, desc="overwrite: every _loaded_at bumped to now", select=OVER, vars={**STEADY, "fp_bump_loaded_at": True}),
    dict(id=20, desc="overwrite: back to real _loaded_at", select=OVER, vars=STEADY),
    dict(id=21, desc="overwrite: same rows again", select=OVER, vars=STEADY),
    # --- schema evolution without replace ------------------------------------------------------
    dict(id=22, desc="evolve: initial without the extra column", select=EVOLVE, flags=["--full-refresh"],
         vars={**STEADY, "fp_extra_column": False}),
    dict(id=23, desc="evolve: column added by ALTER (append_new_columns / sync_all_columns)", select=EVOLVE, vars=STEADY),
    dict(id=24, desc="evolve: column dropped by ALTER (sync_all_columns)", select="fp_evt_sync fp_child_of_sync fp_ledger",
         vars={**STEADY, "fp_drop_column": True}),
    # --- guard must build --------------------------------------------------------------------------
    dict(id=25, desc="guard: initial", select=GUARD, flags=["--full-refresh"], vars=STEADY),
    dict(id=26, desc="guard: unhashable, view, unregistered seed, no Time Travel", select=GUARD, vars=STEADY),
    dict(id=27, desc="guard: seed selected, reloaded by truncate + insert with the same content (child skips)",
         select=f"{GUARD} fp_raw_deleted_b", vars=STEADY),
    dict(id=28, desc="guard switched off: child builds though parents unchanged", select=CORE,
         vars={**STEADY, "fingerprint_skip_unchanged_upstream": False}),
    dict(id=29, desc="late child with no table builds; existing child skips", select=f"{CORE} tag:fp_late",
         vars={**STEADY, "fp_enable_late_child": True}),
    # --- two-step deploy shape: parents in step A, children in step B, one deploy id ---------------
    dict(id=30, desc="two-step: parent unchanged in A, child skipped in B", steps=[
        dict(select="fp_dim_scd2", vars=STEADY),
        dict(select="fp_child_guarded fp_ledger", vars=STEADY),
    ]),
    dict(id=31, desc="two-step: parent modified in A (email case reverted), child built in B", steps=[
        dict(select="fp_dim_scd2", flags=["--full-refresh"], vars={**STEADY, "fp_upper_email": False}),
        dict(select="fp_child_guarded fp_ledger", vars={**STEADY, "fp_upper_email": False}),
    ]),
    # --- seed replaced -------------------------------------------------------------------------------
    dict(id=32, desc="guard: seed selected with --full-refresh, create or replace (child builds)",
         select=f"{GUARD} fp_raw_deleted_b", flags=["--full-refresh"], vars=STEADY),
]


def run(cmd: list[str], *, capture: bool = False) -> subprocess.CompletedProcess:
    print("+", " ".join(cmd), flush=True)
    return subprocess.run(cmd, text=True, capture_output=capture)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--profile", default="default")
    parser.add_argument("--target", default="dev")
    parser.add_argument("--dbt", default="dbt")
    parser.add_argument("--only", help="comma-separated scenario ids")
    parser.add_argument("--from", dest="from_id", type=int, help="first scenario id to run")
    parser.add_argument("--show", action="store_true", help="dump the ledger after every scenario")
    parser.add_argument("--no-seed", action="store_true", help="skip reseeding the fixtures")
    args = parser.parse_args()

    here = Path(__file__).resolve().parent
    common = ["--profile", args.profile, "--target", args.target, "--project-dir", str(here)]
    run_tag = dt.datetime.now().strftime("%Y%m%d%H%M%S")

    chosen = SCENARIOS
    if args.only:
        wanted = {int(x) for x in args.only.split(",")}
        chosen = [s for s in SCENARIOS if s["id"] in wanted]
    if args.from_id:
        chosen = [s for s in chosen if s["id"] >= args.from_id]

    if not args.no_seed:
        print(f"[fingerprint] run tag {run_tag}; seeding fixtures")
        if run([args.dbt, "seed", "--select", "path:seeds/fingerprint", "--full-refresh", *common]).returncode != 0:
            print("[fingerprint] FAIL: seeding")
            return 1

    results: list[tuple[int, str, bool]] = []
    for sc in chosen:
        sid = sc["id"]
        deploy_id = f"fp_{run_tag}_{sid}"
        print(f"\n[fingerprint] ===== {sid}: {sc['desc']} =====", flush=True)

        steps = sc.get("steps") or [dict(select=sc["select"], flags=sc.get("flags", []), vars=sc.get("vars", {}))]
        ok = True
        last_vars = None
        for step in steps:
            step_vars = {
                "fp_iteration": sid,
                "fingerprint": True,
                "fingerprint_skip_unchanged_upstream": True,
                "deploy_id": deploy_id,
                # fp_child_late must never pre-exist, so it gets a fresh alias every run.
                "fp_late_alias": f"fp_child_late_{run_tag}",
                **step.get("vars", {}),
            }
            last_vars = step_vars
            cmd = [args.dbt, "build", "--select", *step["select"].split(), "--vars", json.dumps(step_vars),
                   *step.get("flags", []), *common]
            if run(cmd).returncode != 0:
                ok = False
                break

        print(f"[fingerprint] {'PASS' if ok else 'FAIL'} {sid}", flush=True)
        results.append((sid, sc["desc"], ok))

        if (args.show or not ok) and last_vars is not None:
            run([args.dbt, "show", "--inline",
                 "select node_name, verdict, detail, pre_rows, post_rows, appended_rows, touched_rows, "
                 "touched_old_rows, buckets_hashed, buckets_changed from {{ ref('fp_ledger') }} order by node_name",
                 "--vars", json.dumps(last_vars), "--limit", "40", *common])

    print("\n[fingerprint] summary")
    failed = 0
    for sid, desc, ok in results:
        print(f"  {'PASS' if ok else 'FAIL'}  {sid:>2}  {desc}")
        failed += 0 if ok else 1
    print(f"[fingerprint] {len(results) - failed} passed, {failed} failed (run tag {run_tag})")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
