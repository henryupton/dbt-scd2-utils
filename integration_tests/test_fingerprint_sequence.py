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
Scenarios 33 to 38 are the project's own shapes: a versioned dim, a batch-stamped staging merge
that starts empty, a registry seed with no loaded-at column, and a seed whose CSV the runner
rewrites mid-sequence (FILE_VARIANTS); the originals are restored when the runner exits.
Scenarios 39 to 53 are the guard's remaining branches (ephemeral, multi-parent, transitive,
checksum, no baseline, pending, retry) and the hooks' edge cases (no row timestamps, numeric,
geography and variant columns, quoted identifiers, null and non-tz loaded-at, the scd validity
blind spot). A step marked expect_fail must exit non-zero: it leaves a parent pending on purpose.

Usage:
    ./test_fingerprint_sequence.py [--profile default] [--target dev] [--dbt /path/to/dbt]
                                   [--only 1,2,3] [--from 15] [--show] [--no-seed]
"""

from __future__ import annotations

import argparse
import atexit
import datetime as dt
import json
import subprocess
import sys
from pathlib import Path

CORE = "tag:fingerprint"
OVER = "tag:fp_overwrite"
EVOLVE = "tag:fp_evolve"
GUARD = "tag:fp_guard"
EDP = "tag:fp_edp"
EDGE = "tag:fp_edge"

# Files the runner rewrites mid-sequence: seeds so a reload carries changed content, one guarded model so
# its checksum moves while its parents stay put. A variant stays in place for the scenarios that follow;
# the committed originals are restored when the runner exits.
SEED_LIVE = "seeds/fingerprint/fp_seed_live.csv"
REGISTRY = "seeds/fingerprint/fp_registry.csv"
CHILD_MULTI = "models/fingerprint/fp_edge_child_multi.sql"
FILE_VARIANTS = {
    SEED_LIVE: {
        # customer 3 flips INACTIVE -> ACTIVE with the same timestamps
        "changed": (
            "customer_id,customer_name,email,status,_updated_at,_loaded_at\n"
            "2,Grace Hopper,grace@example.com,ACTIVE,2026-06-15 09:00:00+0000,2026-06-15 09:05:00+0000\n"
            "3,Alan Turing,alan@example.com,ACTIVE,2026-06-16 09:00:00+0000,2026-06-16 09:05:00+0000\n"
            "5,Margaret Hamilton,margaret@example.com,ACTIVE,2026-07-11 09:00:00+0000,2026-07-11 09:05:00+0000\n"
            "6,Barbara Liskov,barbara@example.com,ACTIVE,2026-08-05 09:00:00+0000,2026-08-05 09:05:00+0000\n"
            "7,Frances Allen,frances@example.com,ACTIVE,2026-08-06 09:00:00+0000,2026-08-06 09:05:00+0000\n"
            "2,Grace Hopper,grace@example.com,INACTIVE,2026-08-20 09:00:00+0000,2026-08-20 09:05:00+0000\n"
            "8,Radia Perlman,radia@example.com,ACTIVE,2026-06-20 09:00:00+0000,2026-06-20 09:05:00+0000\n"
        ),
    },
    REGISTRY: {
        # one label changes; with no loaded-at column the fingerprint cannot see it
        "changed": (
            "event_type,label,is_active\n"
            "gen_impression,Generation impression (AI),true\n"
            "item_download,Item download,true\n"
            "subscription_started,Subscription started,true\n"
        ),
    },
    CHILD_MULTI: {
        # the same model with one more comment line: same output, different checksum
        "changed": (
            "{{\n"
            "    config(\n"
            "        materialized='guarded_table',\n"
            "        tags=['fp_edge']\n"
            "    )\n"
            "}}\n"
            "\n"
            "{# Two parents: one blocking verdict on either side builds it. The runner rewrites this file in one #}\n"
            "{# scenario so its checksum moves while both parents stay unchanged. #}\n"
            "{# Rewritten by the runner: this comment is the change. #}\n"
            "select a.customer_id, a.email, b.status\n"
            "from {{ ref('fp_edge_parent_a') }} a\n"
            "join {{ ref('fp_edge_parent_b') }} b on a.customer_id = b.customer_id\n"
        ),
    },
}

# Fixture state after scenario 14, reused by the later groups so they start from a known shape.
STEADY = {"fp_source": "deleted_b", "fp_upper_email": True, "fp_extra_column": True, "fp_null_email_customer": 3}

# The edge group's knobs accumulate; each scenario carries the state the previous one left.
EDGE_41 = {**STEADY, "fp_edge_b_flip": True, "fp_edge_null_ids": [2], "fp_edge_geo_tier": "gold"}
EDGE_42 = {**EDGE_41, "fp_edge_a_flip": True, "fp_edge_quoted_flip": True, "fp_edge_ntz_extra": True,
           "fp_edge_date_extra": ["2026-08-20"]}
EDGE_43 = {**EDGE_42, "fp_edge_null_extra": "ACTIVE", "fp_edge_date_extra": ["2026-08-20", "2026-08-21"]}
EDGE_44 = {**EDGE_43, "fp_edge_null_extra": "INACTIVE"}
EDGE_45 = {k: v for k, v in EDGE_44.items() if k != "fp_edge_null_extra"}
SCD2_PAIR = "fp_edge_scd2 fp_edge_child_of_scd2 fp_ledger"

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
    # --- the project's own shapes: versioned dim, batch-stamped staging merge, registry seed, seed change ---
    # The scd dim is replaced on the initial so a rerun over an existing table reads new, not modified.
    dict(id=33, desc="edp: initial (versioned dim, empty batch stage, registry and live seeds)", select=EDP,
         flags=["--full-refresh"], vars={**STEADY, "fp_batch_stage": 0, "fp_full_refresh_strategy": "replace"}),
    dict(id=34, desc="edp: rows land in the empty stage; seeds reloaded unchanged (registry unhashable)", select=EDP,
         vars={**STEADY, "fp_batch_stage": 1}),
    dict(id=35, desc="edp: late chunk of the current batch lands exactly at the watermark", select=EDP,
         vars={**STEADY, "fp_batch_stage": 2}),
    dict(id=36, desc="edp: next batch", select=EDP, vars={**STEADY, "fp_batch_stage": 3}),
    dict(id=37, desc="edp: seed content changed (live seed row status, registry label)", select=EDP,
         vars={**STEADY, "fp_batch_stage": 3}, files={SEED_LIVE: "changed", REGISTRY: "changed"}),
    dict(id=38, desc="edp: versioned dim gets backdated keys, child builds", select=EDP,
         vars={**STEADY, "fp_source": "inplace", "fp_batch_stage": 3}),
    # --- the guard's remaining branches and the hooks' edge cases ---------------------------------------
    dict(id=39, desc="edge: initial, create or replace everywhere", select=EDGE, flags=["--full-refresh"],
         vars={**STEADY, "fp_full_refresh_strategy": "replace"}),
    dict(id=40, desc="edge: nothing changed (ephemeral looked through, grandchild skips, unhashable children build, "
                     "geography left out, quoted exclusion holds)", select=EDGE, vars=STEADY),
    dict(id=41, desc="edge: one of two parents modified; a row's stamp moves to null; a value inside an object changes",
         select=EDGE, vars=EDGE_41),
    dict(id=42, desc="edge: the ephemeral's parent modified; quoted value flips; ntz row appended one hour up; "
                     "date row on the watermark day", select=EDGE, vars=EDGE_42),
    dict(id=43, desc="edge: a null-stamped row is inserted (not appended); a later date row is appended", select=EDGE,
         vars=EDGE_43),
    dict(id=44, desc="edge: the null-stamped row changes", select=EDGE, vars=EDGE_44),
    dict(id=45, desc="edge: the null-stamped row is removed", select=EDGE, vars=EDGE_45),
    dict(id=46, desc="edge: a guarded child's own SQL changes while its parents stay unchanged", select=EDGE,
         vars=EDGE_45, files={CHILD_MULTI: "changed"}),
    dict(id=47, desc="edge: the changed SQL is now the baseline; it skips again", select=EDGE, vars=EDGE_45),
    dict(id=48, desc="edge: scd2 full refresh moving only _valid_to reads unchanged with the scd columns excluded (pinned)",
         select=SCD2_PAIR, flags=["--full-refresh"], vars={**EDGE_45, "default_valid_to": "2999-01-01 00:00:00"}),
    dict(id=49, desc="edge: the same move with the scd columns included reads modified", select=SCD2_PAIR,
         flags=["--full-refresh"], vars={**EDGE_45, "fingerprint_exclude_scd_columns": False}),
    dict(id=50, desc="edge: a table built with the fingerprint off, then guarded: no baseline, builds once", steps=[
        dict(select="fp_edge_adopted", vars={**EDGE_45, "fingerprint": False, "fp_enable_adopted": True}),
        dict(select=EDGE, vars={**EDGE_45, "fp_enable_adopted": True}),
    ]),
    dict(id=51, desc="edge: the adopted table skips now it has a baseline", select=EDGE,
         vars={**EDGE_45, "fp_enable_adopted": True}),
    dict(id=52, desc="two-step: a parent fails in A and stays pending; its children build in B, the other's skips", steps=[
        dict(select="fp_edge_parent_a fp_edge_parent_b", vars={**EDGE_45, "fp_edge_fail_b": True}, expect_fail=True),
        dict(select="fp_edge_child_of_eph fp_edge_child_multi fp_edge_grandchild fp_ledger", vars=EDGE_45),
    ]),
    dict(id=53, desc="retry: the failed parent is rerun under the same deploy id and settles against the original snapshot",
         steps=[
             dict(select="fp_edge_parent_a fp_edge_parent_b", vars={**EDGE_45, "fp_edge_fail_b": True}, expect_fail=True),
             dict(select="fp_edge_parent_a fp_edge_parent_b", vars=EDGE_45),
             dict(select=EDGE, vars=EDGE_45),
         ]),
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

    originals = {path: (here / path).read_text() for path in FILE_VARIANTS}

    def restore_files() -> None:
        for path, text in originals.items():
            if (here / path).read_text() != text:
                (here / path).write_text(text)
                print(f"[fingerprint] restored {path}", flush=True)

    atexit.register(restore_files)

    results: list[tuple[int, str, bool]] = []
    for sc in chosen:
        sid = sc["id"]
        deploy_id = f"fp_{run_tag}_{sid}"
        print(f"\n[fingerprint] ===== {sid}: {sc['desc']} =====", flush=True)

        for path, variant in sc.get("files", {}).items():
            (here / path).write_text(FILE_VARIANTS[path][variant])
            print(f"[fingerprint] wrote {path} variant {variant!r}", flush=True)

        steps = sc.get("steps") or [dict(select=sc["select"], flags=sc.get("flags", []), vars=sc.get("vars", {}))]
        ok = True
        last_vars = None
        for step in steps:
            step_vars = {
                "fp_iteration": sid,
                "fingerprint": True,
                "fingerprint_skip_unchanged_upstream": True,
                "deploy_id": deploy_id,
                # fp_child_late and fp_edge_adopted must never pre-exist, so they get a fresh alias every run.
                "fp_late_alias": f"fp_child_late_{run_tag}",
                "fp_adopted_alias": f"fp_edge_adopted_{run_tag}",
                **step.get("vars", {}),
            }
            last_vars = step_vars
            cmd = [args.dbt, "build", "--select", *step["select"].split(), "--vars", json.dumps(step_vars),
                   *step.get("flags", []), *common]
            failed = run(cmd).returncode != 0
            if failed != bool(step.get("expect_fail")):
                print(f"[fingerprint] step {'succeeded but was expected to fail' if not failed else 'failed'}", flush=True)
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
