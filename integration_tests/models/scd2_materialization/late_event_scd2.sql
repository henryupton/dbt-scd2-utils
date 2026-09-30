{{
    config(
        materialized='incremental_scd2',
        unique_key=['customer_id'],
        meta={
            'change_columns': {
                'exclude': ['_written_at', '_created_at', '_loaded_at']
            },
            'deleted_at_column': 'deleted_at',
            'backdate_valid_from': true
        }
    )
}}

{#
    backdate_valid_from: a late-arriving earlier-dated row corrects a version's _valid_from
    without re-keying it.

    When identical-content rows collapse into one version, the survivor is still the
    EARLIEST-LOADED row (its _updated_at, _loaded_at and every other column, including any
    key the model hashes from its own event time, are unchanged), but _valid_from becomes
    the earliest event time across the run and the previous version's _valid_to follows it.

    Iteration 1 (initial load) has no late rows, except key 403, which already carries one.
    Iteration 2 (incremental) lands a late ACTIVE row dated 03-10 but loaded 06-05 for keys
    400 (mid history) and 402 (current version): each ACTIVE version keeps its 05-10 survivor
    and moves _valid_from to 03-10. Key 403's batch holds only a second late row (04-01), so
    its earlier 03-10 start is visible only through the persisted _valid_from, and it must
    not regress to 04-01. Key 401 is a monotonic control. Iteration 3 re-runs iteration 2's
    input and must be a no-op. Run via ./test_scd2_sequence.sh 1 3 late_event_scd2
#}

{%- set iteration = var('iteration', 1) -%}

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
from {{ ref('late_event_raw_' ~ iteration) }}
