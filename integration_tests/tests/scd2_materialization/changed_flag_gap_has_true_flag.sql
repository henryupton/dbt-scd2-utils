-- Consistency between version detection and the _changed map.
--
-- A version after the first exists only because the change-detection hash differed from the
-- prior version, so at least one tracked column should read as changed in _changed. This test
-- FAILS when the two disagree: version detection uses generate_surrogate_key (md5 of the
-- varchar cast) while _changed uses IS DISTINCT FROM. Two same-instant TIMESTAMP_TZ values
-- with different UTC offsets hash differently (new version created) but are not distinct
-- (_changed all-false), so the row below is returned and the test fails.

with base as (
    select
        customer_id,
        _updated_at,
        _changed,
        row_number() over (partition by customer_id order by _updated_at) as rn
    from {{ ref('changed_flag_gap_scd2') }}
)

select *
from base
where rn > 1
  and coalesce(_changed:event_at::boolean, false) = false
