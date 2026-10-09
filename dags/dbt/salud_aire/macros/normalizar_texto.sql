{#
  Normaliza texto libre escrito por personas: mayusculas, sin tildes,
  sin espacios al inicio/fin y con espacios internos colapsados.
  'San Martín de  Porres ' -> 'SAN MARTIN DE PORRES'
#}
{% macro normalizar_texto(columna) -%}
    regexp_replace(
        upper(trim(translate({{ columna }}, 'áéíóúüñÁÉÍÓÚÜÑ', 'aeiouunAEIOUUN'))),
        '\\s+', ' '
    )
{%- endmacro %}
