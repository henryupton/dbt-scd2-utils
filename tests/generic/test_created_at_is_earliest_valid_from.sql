{% test created_at_is_earliest_valid_from(model, key_columns, created_at_column, valid_from_column) %}

with earliest_valid_from as (
    select 
        {{ dbt_scd2_utils.get_quoted_csv(key_columns) }},
        min({{ valid_from_column }}) as min_valid_from
    from {{ model }}
    group by {{ dbt_scd2_utils.get_quoted_csv(key_columns) }}
),
created_at_validation as (
    select 
        m.{{ dbt_scd2_utils.get_quoted_csv(key_columns) }},
        m.{{ created_at_column }},
        e.min_valid_from
    from {{ model }} m
    inner join earliest_valid_from e
        on {% for key_col in key_columns -%}
            m.{{ key_col }} = e.{{ key_col }}
            {%- if not loop.last %} and {% endif -%}
        {%- endfor %}
    where m.{{ created_at_column }} != e.min_valid_from
)
select * from created_at_validation

{% endtest %}