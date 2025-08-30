{% test first_record_insert(model, key_columns, valid_from_column, change_type_column) %}

with first_records as (
    select 
        {{ dbt_scd2_utils.get_quoted_csv(key_columns) }},
        {{ change_type_column }},
        row_number() over (partition by {{ dbt_scd2_utils.get_quoted_csv(key_columns) }} order by {{ valid_from_column }}) as rn
    from {{ model }}
),
invalid_first_records as (
    select 
        {{ dbt_scd2_utils.get_quoted_csv(key_columns) }},
        {{ change_type_column }}
    from first_records
    where rn = 1
        and {{ change_type_column }} != 'I'
)
select * from invalid_first_records

{% endtest %}