{{
    config(
        materialized='incremental_scd2',
        unique_key=['customer_id'],
        meta={
            'track_previous_version': true,
            'track_changed_columns': true,
            'change_columns': {
                'exclude': ['_written_at', '_created_at']
            }
        }
    )
}}

{#
    Reproduces the version-detection vs _changed semantics gap.

    Customer 1 has two versions differing only in the tracked column `event_at`, but the
    two event_at values are the SAME instant with different UTC offsets
    (2024-01-01 10:00:00 +0000 and 2024-01-01 11:00:00 +0100).

    generate_surrogate_key (md5 of the varchar cast) sees different strings
    ("...10:00:00.000 Z" vs "...11:00:00.000 +0100") and creates a new version, while
    _changed uses IS DISTINCT FROM (same instant -> false). So the second version exists
    yet its _changed map is all-false, which the changed_flag_gap_has_true_flag test
    asserts against.
#}

select
    customer_id,
    event_at_str::timestamp_tz as event_at,
    _created_at::timestamp_tz as _created_at,
    _updated_at::timestamp_tz as _updated_at,
    sysdate() as _written_at
from {{ ref('changed_flag_gap_raw_1') }}
