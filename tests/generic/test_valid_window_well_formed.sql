{#
  Asserts every row's validity window is well-formed: valid_from and valid_to are
  both non-null and valid_from is strictly before valid_to.

  Applies to all SCD types — for types 0 and 1 valid_to is the open-ended default
  (so valid_from is well before it); for type 2 each version spans a finite window.
  Fails for any row with a null bound or a non-positive-length window.
#}

{% test valid_window_well_formed(model, valid_from_column, valid_to_column) %}

select *
from {{ model }}
where {{ valid_from_column }} is null
   or {{ valid_to_column }} is null
   or {{ valid_from_column }} >= {{ valid_to_column }}

{% endtest %}
