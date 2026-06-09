{{
    config(
        materialized='incremental_scd2',
        unique_key=['customer_id'],
        meta={
            'deleted_at_column': 'deleted_at'
        }
    )
}}

{#
    Born-deleted edge case: a key whose first and only version arrives already
    soft-deleted (ingestion started after the delete happened). It must be
    recorded as a deletion ('D'), valid from deleted_at, and current.

    created_at_column resolves to _created_at from dbt_project.yml vars, so the
    created_at override path in get_valid_from_sql is exercised. _created_at,
    deleted_at and _updated_at are deliberately distinct in the seeds.

    Iteration 1 (full refresh -> initial load): key 100 arrives already deleted.
    Iteration 2 (incremental): a second born-deleted key 101 arrives while the
    table already exists, exercising the MERGE insert path. Key 100 is unchanged.
#}

{%- set iteration = var('iteration', 1) | int -%}
{%- set seed_iteration = 1 if iteration < 2 else 2 -%}

select
    customer_id,
    customer_name,
    email,
    status,
    deleted_at::timestamp_tz as deleted_at,
    _created_at::timestamp_tz as _created_at,
    _updated_at::timestamp_tz as _updated_at
from {{ ref('born_deleted_raw_' ~ seed_iteration) }}
