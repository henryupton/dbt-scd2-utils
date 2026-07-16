-- All-switches-on coverage: prev_changed_scd2 enables _previous, _changed AND _checksum, so
-- this exercises the three optional audit columns coexisting in scd2_versions and the
-- redundant_versions union. _checksum must equal the md5 fingerprint of the row's own
-- business columns (all non-audit, non-lifecycle columns, including the key and _written_at),
-- listed in the alphabetical order the implementation hashes.
select customer_id, _updated_at, _checksum
from {{ ref('prev_changed_scd2') }}
where _checksum is distinct from {{ dbt_utils.generate_surrogate_key(['customer_id', 'customer_name', 'email', 'status', '_written_at'] | sort) }}
