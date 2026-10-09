{#
  Test generico propio: la combinacion de columnas debe ser unica (clave
  natural compuesta). Equivalente a dbt_utils.unique_combination_of_columns,
  sin depender de paquetes externos (dbt deps en tiempo de ejecucion).
#}
{% test combinacion_unica(model, columnas) %}

select
    {{ columnas | join(', ') }},
    count(*) as repeticiones
from {{ model }}
group by {{ columnas | join(', ') }}
having count(*) > 1

{% endtest %}
