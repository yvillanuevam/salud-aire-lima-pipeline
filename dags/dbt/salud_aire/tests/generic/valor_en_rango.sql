{#
  Test generico propio: el valor debe estar dentro de un rango fisicamente
  posible. Los nulos se ignoran (para eso existe not_null).
#}
{% test valor_en_rango(model, column_name, minimo, maximo) %}

select {{ column_name }}
from {{ model }}
where {{ column_name }} is not null
  and ({{ column_name }} < {{ minimo }} or {{ column_name }} > {{ maximo }})

{% endtest %}
