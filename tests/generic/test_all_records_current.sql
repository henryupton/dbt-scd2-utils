{#
  SCD Type 1 invariant: every row is the current version.

  SCD1 keeps exactly one row per key, so the is_current flag should be true for
  every record. Fails for any row where it is not.
#}

{% test all_records_current(model, current_column) %}

select *
from {{ model }}
where {{ current_column }} != true
   or {{ current_column }} is null

{% endtest %}
