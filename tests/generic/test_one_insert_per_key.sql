{% test one_insert_per_key(model, key_columns, change_type_column='_change_type') %}

with insert_records as (
    select
        {{ dbt_scd2_utils.get_quoted_csv(key_columns) }},
        count(*) as insert_count
    from {{ model }}
    where {{ change_type_column }} = 'I'
    group by {{ dbt_scd2_utils.get_quoted_csv(key_columns) }}
),
invalid_keys as (
    select
        {{ dbt_scd2_utils.get_quoted_csv(key_columns) }},
        insert_count
    from insert_records
    where insert_count > 1
)
select * from invalid_keys

{% endtest %}