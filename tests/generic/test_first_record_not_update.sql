{% test first_record_not_update(model, key_columns, valid_from_column='_valid_from', change_type_column='_change_type') %}
{#
    A key's first record (earliest valid_from) must not be an Update ('U').
    The first record is either an Insert ('I') for a normally-created entity, or
    a Delete ('D') for a born-deleted entity (ingestion started after the soft
    delete). An Update as the first record implies a prior version that does not
    exist, which is invalid.

    Assumes valid_from is unique per key (guaranteed by no_validity_overlaps);
    otherwise the row_number tiebreak for the first record is non-deterministic.
#}

with first_records as (
    select
        {{ dbt_scd2_utils.get_quoted_csv(key_columns) }},
        {{ change_type_column }},
        row_number() over (partition by {{ dbt_scd2_utils.get_quoted_csv(key_columns) }} order by {{ valid_from_column }}) as rn
    from {{ model }}
),
invalid_first_records as (
    select
        {{ dbt_scd2_utils.get_quoted_csv(key_columns) }},
        {{ change_type_column }}
    from first_records
    where rn = 1
        and {{ change_type_column }} = 'U'
)
select * from invalid_first_records

{% endtest %}
