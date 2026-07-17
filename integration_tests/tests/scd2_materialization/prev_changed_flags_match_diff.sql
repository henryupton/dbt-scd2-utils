-- Fails if a _changed flag disagrees with the actual diff between the row and its prior version.
with base as (
    select
        customer_id,
        _updated_at,
        _changed,
        row_number() over (partition by customer_id order by _updated_at) as rn,
        (cast(customer_name as varchar) is distinct from lag(cast(customer_name as varchar)) over (partition by customer_id order by _updated_at)) as customer_name_changed,
        (cast(email as varchar)         is distinct from lag(cast(email as varchar))         over (partition by customer_id order by _updated_at)) as email_changed,
        (cast(status as varchar)        is distinct from lag(cast(status as varchar))        over (partition by customer_id order by _updated_at)) as status_changed
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
