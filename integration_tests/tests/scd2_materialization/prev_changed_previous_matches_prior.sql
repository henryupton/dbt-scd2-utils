-- Fails if _previous does not match the actual prior version's tracked columns.
with expected as (
    select
        customer_id,
        _updated_at,
        _previous,
        row_number() over (partition by customer_id order by _updated_at) as rn,
        lag(customer_name) over (partition by customer_id order by _updated_at) as prev_customer_name,
        lag(email)         over (partition by customer_id order by _updated_at) as prev_email,
        lag(status)        over (partition by customer_id order by _updated_at) as prev_status
    from {{ ref('prev_changed_scd2') }}
)

select *
from expected
where rn > 1
  and (
       _previous:customer_name::string is distinct from prev_customer_name
    or _previous:email::string         is distinct from prev_email
    or _previous:status::string        is distinct from prev_status
  )
