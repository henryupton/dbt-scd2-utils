{{
    config(
        materialized='incremental_scd2',
        unique_key=['customer_id']
    )
}}

select
    customer_id,
    credit_rating,
    credit_score,
    _updated_at
from {{ ref('credit_ratings_source') }}