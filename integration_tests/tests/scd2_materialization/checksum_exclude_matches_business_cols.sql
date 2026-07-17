-- checksum_exclude: batch_id is present in the model but listed in checksum_exclude, so it
-- must NOT be folded into the fingerprint. _checksum must equal the md5 of the remaining
-- business columns (customer_id, customer_name, email, status) in the alphabetical order the
-- implementation hashes. If the exclude were ignored, batch_id would sort in ahead of
-- customer_id and the checksum would not match this oracle, so this row would be returned.
-- It also implies the two same-content versions of customer 1 (which differ only in
-- batch_id) share one checksum, since their oracle values are identical.
select customer_id, _updated_at, _checksum
from {{ ref('checksum_exclude_scd2') }}
where _checksum is distinct from {{ dbt_utils.generate_surrogate_key(['customer_id', 'customer_name', 'email', 'status']) }}
