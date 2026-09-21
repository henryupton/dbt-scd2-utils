{{
    config(
        materialized='incremental_scd2',
        unique_key=['customer_id'],
        meta={
            'change_columns': {
                'exclude': ['_written_at', '_created_at', '_loaded_at']
            },
            'deleted_at_column': 'deleted_at'
        }
    )
}}

{#
    Regression reproduction: the initial-load and incremental paths must keep the
    SAME survivor when identical-content versions collapse into one run.

    A bulk reload re-delivers an EARLIER-dated version (_updated_at 2024-01-15) of
    content that already arrived in real time (_updated_at 2024-03-10), but lands it
    LATER (_loaded_at 2024-03-13 vs 2024-03-10). The two rows share a hash, so they
    are one content run. The canonical survivor is the EARLIEST-LOADED row
    (2024-03-10) on both paths. The initial load used to keep the earliest-updated_at
    row instead, back-dating the version -- and the predecessor's _valid_to -- to a
    row that did not exist in the warehouse until 2024-03-13, so a full refresh and
    the incremental runs after it maintained different histories.

    Key 300 has a successor version after the collapsed run, key 302 has the
    collapsed run as its current version, key 301 is a monotonic control where
    earliest-loaded and earliest-updated_at coincide.

    The single raw seed is read on every iteration: iteration 1 is the initial load,
    iteration 2 an incremental run over identical input that must be a no-op
    (redelivered_history_expected_1 == redelivered_history_expected_2). Run via
    ./test_scd2_sequence.sh 1 2 redelivered_history_scd2
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
from {{ ref('redelivered_history_raw_1') }}
