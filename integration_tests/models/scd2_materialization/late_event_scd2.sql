{{
    config(
        materialized='incremental_scd2',
        unique_key=['customer_id'],
        meta={
            'change_columns': {
                'exclude': ['_written_at', '_created_at', '_loaded_at']
            },
            'deleted_at_column': 'deleted_at',
            'run_survivor': 'earliest_updated'
        }
    )
}}

{#
    run_survivor: earliest_updated — the event clock dates the version.

    A genuinely late-arriving event (_updated_at 2024-03-10, _loaded_at 2024-05-13)
    collides with an on-time repeat observation of the same content (_updated_at
    2024-05-10, _loaded_at 2024-05-10). The two share a hash, so they are one content
    run. Under the default (earliest_loaded) the on-time row would survive and the
    version would be dated 2024-05-10 — two months after the state actually changed.
    Under earliest_updated the late event wins: the version is dated 2024-03-10 and
    keeps the survivor's own _loaded_at (2024-05-13), so downstream loaded_at cursors
    still see the late arrival.

    Key 400 has a successor version after the collapsed run, key 402 has the
    collapsed run as its current version, key 401 is a monotonic control where the
    two orders coincide.

    The single raw seed is read on every iteration: iteration 1 is the initial load,
    iteration 2 an incremental run over identical input that must be a no-op
    (late_event_expected_1 == late_event_expected_2), proving the two paths keep the
    same survivor in this mode too. Run via ./test_scd2_sequence.sh 1 2 late_event_scd2
#}

select
    customer_id,
    customer_name,
    email,
    status,
    deleted_at::timestamp_tz as deleted_at,
    _created_at::timestamp_tz as _created_at,
    _updated_at::timestamp_tz as _updated_at,
    _loaded_at::timestamp_tz as _loaded_at,
    sysdate() as _written_at
from {{ ref('late_event_raw_1') }}
