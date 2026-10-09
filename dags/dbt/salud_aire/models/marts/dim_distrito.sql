-- dim_distrito: dimension de distritos para el consumo (BI).

select
    ubigeo,
    distrito,
    zona,
    latitud,
    longitud
from {{ ref('distritos_lima') }}
