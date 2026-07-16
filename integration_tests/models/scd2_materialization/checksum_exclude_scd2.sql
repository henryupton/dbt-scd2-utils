{{
    config(
        materialized='incremental_scd2',
        unique_key=['customer_id'],
        meta={
            'track_checksum': true,
            'checksum_exclude': ['batch_id']
        }
    )
}}

{#
    Exercises checksum_exclude. batch_id is a volatile column that varies between
    versions but is listed in checksum_exclude, so it must NOT contribute to _checksum.

    Iteration 1 (full refresh): customer 1 has two versions with identical business
    content differing only in batch_id, so a new version is created (batch_id is still
    in change detection) yet both versions must carry the same _checksum.
    Iteration 2 (incremental): customer 1 gains a third version with genuinely changed
    business content, so its _checksum changes to match.

    Business columns are selected in non-alphabetical order so the test also confirms
    the checksum sort.
#}

{%- set iteration = var('iteration', 1) | int -%}
{%- set seed_iteration = iteration if iteration <= 2 else 2 -%}

select
    status,
    batch_id,
    email,
    customer_name,
    customer_id,
    _updated_at::timestamp_tz as _created_at,
    _updated_at::timestamp_tz as _updated_at
from {{ ref('checksum_exclude_raw_' ~ seed_iteration) }}
