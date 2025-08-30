{% macro get_change_type_sql(unique_keys_csv, updated_at_col) %}
case when row_number() over (partition by {{ unique_keys_csv }} order by {{ updated_at_col }}) = 1 then 'I' else 'U' end
{% endmacro %}