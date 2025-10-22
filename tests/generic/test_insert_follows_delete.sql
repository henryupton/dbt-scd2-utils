{% test insert_follows_delete(model, key_columns, change_type_column='_change_type', valid_from_column='_valid_from') %}
{#
    Test to ensure that after a deletion ('D'), if there is a next record it must be either:
    - An insert ('I') for resurrection
    - Another delete ('D') for multiple deletion events

    This test verifies resurrection handling: when a record is deleted and then comes back,
    it must be marked as a new insert, not an update. Consecutive deletes are allowed to
    handle scenarios where multiple deletion events are received from upstream systems.

    Args:
        model: The model to test
        key_columns (list): Business key columns to identify unique entities
        change_type_column (string): Column containing change type ('I', 'U', 'D')
        valid_from_column (string): Column containing the start of validity window

    Example:
        Valid sequences:
        - Record 1: change_type = 'I' (initial insert)
        - Record 2: change_type = 'D' (deletion)
        - Record 3: change_type = 'I' (resurrection - VALID)

        - Record 1: change_type = 'I' (initial insert)
        - Record 2: change_type = 'D' (deletion)
        - Record 3: change_type = 'D' (multiple deletion events - VALID)

        Invalid sequence:
        - Record 1: change_type = 'I' (initial insert)
        - Record 2: change_type = 'D' (deletion)
        - Record 3: change_type = 'U' (INVALID - should be 'I' or 'D')
#}

with ordered_records as (
    select
        {% for key in key_columns %}{{ key }}{{ ", " if not loop.last }}{% endfor %},
        {{ change_type_column }} as current_change_type,
        lag({{ change_type_column }}) over(partition by {{ dbt_scd2_utils.get_quoted_csv(key_columns) }} order by {{ valid_from_column }}) as prev_change_type,
        {{ valid_from_column }}
    from {{ model }}
),

invalid_records as (
    select
        {% for key in key_columns %}{{ key }}{{ ", " if not loop.last }}{% endfor %},
        {{ valid_from_column }},
        current_change_type,
        prev_change_type
    from ordered_records
    where prev_change_type = 'D'
        and current_change_type not in ('I', 'D')  -- Allow consecutive deletes and inserts after delete
)

select * from invalid_records

{% endtest %}