-- Fails if a _changed flag disagrees with the actual diff between the row and its prior version.
with base as (
    select
        customer_id,
        _updated_at,
        _changed,
        row_number() over (partition by customer_id order by _updated_at) as rn,
        (customer_name is distinct from lag(customer_name) over (partition by customer_id order by _updated_at)) as customer_name_changed,
        (email         is distinct from lag(email)         over (partition by customer_id order by _updated_at)) as email_changed,
        (status        is distinct from lag(status)        over (partition by customer_id order by _updated_at)) as status_changed
    from {{ ref('prev_changed_scd2') }}
)

select *
from base
where rn > 1
  and (
       _changed:customer_name::boolean is distinct from customer_name_changed
    or _changed:email::boolean         is distinct from email_changed
    or _changed:status::boolean        is distinct from status_changed
  )
