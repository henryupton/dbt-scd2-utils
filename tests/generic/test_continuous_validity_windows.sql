{% test continuous_validity_windows(model, key_columns, valid_from_column, valid_to_column) %}

with sequenced_records as (
    select 
        {{ dbt_scd2_utils.get_quoted_csv(key_columns) }},
        {{ valid_from_column }},
        {{ valid_to_column }},
        lead({{ valid_from_column }}) over (partition by {{ dbt_scd2_utils.get_quoted_csv(key_columns) }} order by {{ valid_from_column }}) as next_valid_from
    from {{ model }}
),
gaps as (
    select 
        {{ dbt_scd2_utils.get_quoted_csv(key_columns) }},
        {{ valid_from_column }},
        {{ valid_to_column }},
        next_valid_from
    from sequenced_records
    where next_valid_from is not null
        and {{ valid_to_column }} != next_valid_from
)
select * from gaps

{% endtest %}