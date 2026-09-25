{{
    config(
        materialized='guarded_table',
        tags=['fp_edp']
    )
}}

{# Parent is a registry seed with no loaded-at column: unhashable whatever its content, so this always builds. #}
select *
from {{ ref('fp_registry') }}
