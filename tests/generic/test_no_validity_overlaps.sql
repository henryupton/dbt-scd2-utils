{% test no_validity_overlaps(model, key_columns, valid_from_column, valid_to_column) %}

with overlaps as (
    select 
        {{ dbt_scd2_utils.get_quoted_csv(key_columns, 'a.') }},
        a.{{ valid_from_column }} as a_valid_from,
        a.{{ valid_to_column }} as a_valid_to,
        b.{{ valid_from_column }} as b_valid_from, 
        b.{{ valid_to_column }} as b_valid_to
    from {{ model }} a
    join {{ model }} b 
        on {%- for key_col in key_columns %}
            a.{{ key_col }} = b.{{ key_col }}{{ " and " if not loop.last }}
        {%- endfor %}
        and a.{{ valid_from_column }} != b.{{ valid_from_column }}
    where a.{{ valid_from_column }} < b.{{ valid_to_column }} 
        and b.{{ valid_from_column }} < a.{{ valid_to_column }}
)
select * from overlaps

{% endtest %}