{% test last_record_update_or_delete(model, key_columns, valid_from_column, change_type_column) %}

with record_counts as (
    select 
        {{ dbt_scd2_utils.get_quoted_csv(key_columns) }},
        count(*) as record_count
    from {{ model }}
    group by {{ dbt_scd2_utils.get_quoted_csv(key_columns) }}
),
last_records as (
    select 
        {{ dbt_scd2_utils.get_quoted_csv(key_columns) }},
        {{ change_type_column }},
        row_number() over (partition by {{ dbt_scd2_utils.get_quoted_csv(key_columns) }} order by {{ valid_from_column }} desc) as rn
    from {{ model }}
),
invalid_last_records as (
    select 
        {{ dbt_scd2_utils.get_quoted_csv(key_columns, 'lr.') }},
        lr.{{ change_type_column }}
    from last_records lr
    join record_counts rc on {%- for key_col in key_columns %}
        lr.{{ key_col }} = rc.{{ key_col }}{{ " and " if not loop.last }}
    {%- endfor %}
    where lr.rn = 1
        and rc.record_count > 1  -- Only check if there are multiple records
        and lr.{{ change_type_column }} not in ('U', 'D')
)
select * from invalid_last_records

{% endtest %}