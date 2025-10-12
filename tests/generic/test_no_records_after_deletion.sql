{% test no_records_after_deletion(model, key_columns, deleted_at_column, valid_from_column, valid_to_column) %}
{#
    Test to ensure proper deletion handling in SCD2 tables with resurrection support.

    This test verifies that when a record is marked as deleted (deleted_at is not null),
    its valid_from equals the deleted_at timestamp. The deletion record represents the state
    of being deleted and should span from the deletion time until either resurrection or forever.

    Args:
        model: The model to test
        key_columns (list): Business key columns to identify unique entities
        deleted_at_column (string): Column containing deletion timestamp
        valid_from_column (string): Column containing the start of validity window
        valid_to_column (string): Column containing the end of validity window

    Example:
        For a product with:
        - Record 1: valid_from = 2024-01-01, valid_to = 2024-01-05, deleted_at = NULL (VALID)
        - Record 2: valid_from = 2024-01-10, valid_to = 2024-01-15, deleted_at = 2024-01-10 (VALID - deletion spans until resurrection)
        - Record 3: valid_from = 2024-01-15, valid_to = 2999-12-31, deleted_at = NULL (VALID - resurrected)

        Invalid:
        - Record with deleted_at = 2024-01-10 but valid_from = 2024-01-05 (should be 2024-01-10)
#}

with deleted_records_with_wrong_valid_from as (
    select
        {% for key in key_columns %}{{ key }}{{ ", " if not loop.last }}{% endfor %},
        {{ deleted_at_column }} as deleted_at,
        {{ valid_from_column }} as valid_from
    from {{ model }}
    where {{ deleted_at_column }} is not null
        and {{ valid_from_column }} != {{ deleted_at_column }}
)

select *
from deleted_records_with_wrong_valid_from

{% endtest %}