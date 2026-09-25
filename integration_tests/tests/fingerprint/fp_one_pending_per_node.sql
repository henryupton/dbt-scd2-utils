{{
    config(
        tags=['fingerprint', 'fp_overwrite', 'fp_evolve', 'fp_guard', 'fp_late', 'fp_edp', 'fp_edge']
    )
}}

-- depends_on: {{ ref('fp_ledger') }}

{# Registration is idempotent per deploy and node: a later step or a retry of the same deploy adds no second pending row. #}
select node_id, count_if(verdict = 'pending') as pending_rows, count(*) as ledger_rows
from {{ dbt_scd2_utils.fingerprint_relation('deploy_node') }}
where deploy_id = '{{ var("deploy_id", invocation_id) }}'
group by node_id
having count_if(verdict = 'pending') <> 1
