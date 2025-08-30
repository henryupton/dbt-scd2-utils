{% test one_current_per_key(model, key_columns, current_column) %}

select 
    {{ dbt_scd2_utils.get_quoted_csv(key_columns) }},
    count(*) as current_record_count
from {{ model }}
where {{ current_column }} = true
group by {{ dbt_scd2_utils.get_quoted_csv(key_columns) }}
having count(*) != 1

{% endtest %}