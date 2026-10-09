{#
  Test generico propio: un conteo o una medicion nunca puede ser negativa.
  Devuelve las filas que lo violan (dbt falla si hay al menos una).
#}
{% test no_negativo(model, column_name) %}

select {{ column_name }}
from {{ model }}
where {{ column_name }} < 0

{% endtest %}
