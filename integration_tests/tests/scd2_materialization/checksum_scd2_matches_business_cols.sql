select customer_id, _updated_at, _checksum
from {{ ref('checksum_scd2') }}
where _checksum is distinct from {{ dbt_utils.generate_surrogate_key(['customer_id', 'customer_name', 'email', 'status']) }}
