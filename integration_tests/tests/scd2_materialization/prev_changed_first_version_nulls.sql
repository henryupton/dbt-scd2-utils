-- Fails if a key's first version has non-null objects, or any later version has null objects.
with base as (
    select
        customer_id,
        row_number() over (partition by customer_id order by _updated_at) as rn,
        _previous,
        _changed
    from {{ ref('prev_changed_scd2') }}
)

select *
from base
where (rn = 1 and (_previous is not null or _changed is not null))
   or (rn > 1 and (_previous is null or _changed is null))
