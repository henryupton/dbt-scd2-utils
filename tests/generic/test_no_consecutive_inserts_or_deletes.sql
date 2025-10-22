{% test no_consecutive_inserts_or_deletes(model, key_columns, change_type_column='_change_type', valid_from_column='_valid_from') %}
{#
    Test to ensure no consecutive insert ('I') or delete ('D') records exist.

    This test verifies proper change type sequencing:
    - Insert ('I') must be followed by Update ('U') or Delete ('D'), never another Insert
    - Delete ('D') can be followed by Insert ('I') for resurrection, or another Delete ('D') for multiple deletion events
    - Delete ('D') must NOT be followed by Update ('U') - resurrections must use Insert
    - Update ('U') can be followed by Update, or Delete

    Args:
        model: The model to test
        key_columns (list): Business key columns to identify unique entities
        change_type_column (string): Column containing change type ('I', 'U', 'D')
        valid_from_column (string): Column containing the start of validity window

    Example:
        Invalid sequences:
        - 'I' followed by 'I' (should have 'U' or 'D' between)
        - 'D' followed by 'U' (should have 'I' for resurrection)

        Valid sequences:
        - 'I' -> 'U' -> 'D' -> 'I' -> 'U' (normal with resurrection)
        - 'I' -> 'U' -> 'D' -> 'D' (multiple deletion events)
#}

{%- set invalid_sequences = [
    ('I', 'I', 'Consecutive INSERTs detected'),
    ('D', 'U', 'DELETE followed by UPDATE (should be INSERT for resurrection)')
] -%}

with ordered_records as (
    select
        {% for key in key_columns %}{{ key }}{{ ", " if not loop.last }}{% endfor %},
        {{ change_type_column }} as current_change_type,
        lag({{ change_type_column }}) over(partition by {{ dbt_scd2_utils.get_quoted_csv(key_columns) }} order by {{ valid_from_column }}) as prev_change_type,
        {{ valid_from_column }}
    from {{ model }}
),

invalid_records as (
    {% for prev, curr, reason in invalid_sequences %}
    select
        {% for key in key_columns %}{{ key }}{{ ", " if not loop.last }}{% endfor %},
        {{ valid_from_column }},
        current_change_type,
        prev_change_type,
        '{{ reason }}' as violation_reason
    from ordered_records
    where prev_change_type = '{{ prev }}'
        and current_change_type = '{{ curr }}'
    {% if not loop.last %}
    union all
    {% endif %}
    {% endfor %}
)

select * from invalid_records

{% endtest %}