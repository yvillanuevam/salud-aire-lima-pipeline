-- int_pm25_distrito_dia: a cada distrito le asigna la estacion de monitoreo
-- MAS CERCANA que tenga dato confiable ese dia (cobertura >= cobertura_minima_pct)
-- y que este a menos de radio_max_estacion_km de su centroide.
-- HAVERSINE es una funcion nativa de Snowflake (distancia en km sobre la esfera).

with mediciones as (

    select *
    from {{ ref('stg_mediciones_aire') }}
    where cobertura_pct >= {{ var('cobertura_minima_pct') }}

),

distritos as (

    select * from {{ ref('distritos_lima') }}

),

candidatas as (

    select
        m.fecha,
        d.ubigeo,
        m.estacion_id,
        m.estacion_nombre,
        m.pm25_promedio_24h,
        m.fuente,
        haversine(d.latitud, d.longitud, m.latitud, m.longitud) as distancia_km
    from distritos as d
    cross join mediciones as m

)

select
    fecha,
    ubigeo,
    estacion_id                 as estacion_referencia_id,
    estacion_nombre             as estacion_referencia,
    round(distancia_km, 2)      as distancia_estacion_km,
    pm25_promedio_24h,
    fuente                      as fuente_calidad_aire
from candidatas
where distancia_km <= {{ var('radio_max_estacion_km') }}
qualify row_number() over (partition by fecha, ubigeo order by distancia_km, estacion_id) = 1
