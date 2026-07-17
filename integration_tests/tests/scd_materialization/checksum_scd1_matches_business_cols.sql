select customer_id, _checksum
from {{ ref('checksum_scd1') }}
where _checksum is distinct from {{ dbt_scd2_utils.to_uuid(dbt_utils.generate_surrogate_key(['customer_id', 'customer_name', 'email', 'status'])) }}
