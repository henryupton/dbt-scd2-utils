{% test latest_row_is_current(model, key_columns, valid_from_column, current_column) %}

with latest_rows as (
    select 
        {{ dbt_scd2_utils.get_quoted_csv(key_columns) }},
        {{ current_column }},
        row_number() over (partition by {{ dbt_scd2_utils.get_quoted_csv(key_columns) }} order by {{ valid_from_column }} desc) as rn
    from {{ model }}
),
incorrect_current_flags as (
    select 
        {{ dbt_scd2_utils.get_quoted_csv(key_columns) }},
        {{ current_column }}
    from latest_rows
    where rn = 1
        and {{ current_column }} != true
)
select * from incorrect_current_flags

{% endtest %}
