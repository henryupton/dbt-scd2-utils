{#
    Generates SQL for joining multiple SCD Type 2 tables on a temporal spine.

    Creates a temporal spine based on all valid_from and valid_to timestamps from the
    provided relations, then performs temporal joins to reconstruct the state of all
    tables at each point in time.

    **Args:**
    - `relations` (list): List of relation objects to join temporally
    - `join_keys` (list): List of business key columns to join on. Supports composite keys.
        Example: ['customer_id'] or ['customer_id', 'order_id']

    **Returns:**
    - SELECT SQL statement that joins all relations on the temporal spine

    **Example:**
    For customer and address SCD2 tables, this will create time-based snapshots
    showing how both tables looked at each point when either table changed.
#}

{% macro scd2_join(relations, join_keys) %}
    {# Create comma-separated string for SQL using get_quoted_csv #}
    {%- set join_keys_csv = dbt_scd2_utils.get_quoted_csv(join_keys) -%}

    with
        {# Collect all timestamps from valid_from and valid_to columns across relations #}
        distinct_updates as (
            {% for relation in relations %}
            select {{ join_keys_csv }}, _valid_from::timestamp_tz as _updated_at from {{ relation }}
            {% if not loop.last %}union{%- endif %}
            {% endfor %}
        ),

        {# Create temporal spine with valid_from and valid_to ranges #}
        temporal_spine as (
            select
                {{ join_keys_csv }},
                {{ dbt_scd2_utils.get_is_current_sql(join_keys_csv, '_updated_at') }} as _is_current,
                {{ dbt_scd2_utils.get_valid_from_sql('_updated_at') }} as _valid_from,
                {{ dbt_scd2_utils.get_valid_to_sql(join_keys_csv, '_updated_at', var('default_valid_to')) }} as _valid_to
            from distinct_updates
        )

    select
        {{ dbt_scd2_utils.get_quoted_csv(join_keys, 'spine.') }},
        {%- for relation in relations %}
            {%- for column in adapter.get_columns_in_relation(relation) %}
                {%- if column.name.upper() not in (join_keys | map('upper') | list) and column.name.upper() not in ['_VALID_FROM', '_VALID_TO', '_IS_CURRENT', '_UPDATED_AT', '_CHANGE_TYPE'] %}
        {{ relation.name }}.{{ column.name }},
                {%- endif %}
            {%- endfor %}
        {%- endfor %}
        spine._is_current,
        spine._valid_from,
        spine._valid_to
    from temporal_spine as spine

    {% for relation in relations %}
    left join {{ relation }} as {{ relation.name }}
        on {% for key in join_keys %}spine.{{ key }} = {{ relation.name }}.{{ key }}{{ " and " if not loop.last }}{% endfor %}
        and spine._valid_from >= {{ relation.name }}._valid_from
        and spine._valid_to <= {{ relation.name }}._valid_to
    {% endfor %}

    where spine._valid_from < spine._valid_to
{% endmacro %}
