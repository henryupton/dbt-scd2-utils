{{
    config(
        materialized='overwrite_table',
        tags=['fp_edge'],
        meta={
            'fingerprint_loaded_at': 'Landed At',
            'fingerprint_exclude': ['Note Col']
        }
    )
}}

{# Mixed-case identifiers with spaces, a loaded-at column named per model and a per-model exclusion that #}
{# changes every build. Anything unquoted or case-folded in the hooks fails here. #}
select
    customer_id as "Customer Id",
    {% if var('fp_edge_quoted_flip', false) %}lower(status){% else %}status{% endif %} as "Status",
    email as "eMail",
    sysdate()::timestamp_tz as "Note Col",
    _loaded_at as "Landed At"
from ({{ fp_customer_rows() }})
