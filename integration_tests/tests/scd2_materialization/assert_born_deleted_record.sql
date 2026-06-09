-- Born-deleted edge case: a key whose only-ever row arrives already soft-deleted
-- must be recorded as a deletion, valid from the deletion timestamp, and current.
select
    customer_id,
    _change_type,
    _valid_from,
    deleted_at,
    _is_current
from {{ ref('born_deleted_scd2') }}
where customer_id in (100, 101)
  and not (
        _change_type = 'D'
    and _valid_from = deleted_at
    and _is_current = true
  )
