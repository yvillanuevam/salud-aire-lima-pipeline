-- stg_mediciones_aire: tipado de las mediciones diarias de calidad del aire.
-- Un mismo dia puede tener varios sensores PM2.5 en la misma estacion: se
-- promedian para tener UNA medicion por estacion y dia.

with origen as (

    select * from {{ source('raw', 'mediciones_aire') }}
    where lower(parametro) = 'pm25'

),

por_estacion as (

    select
        fecha::date                          as fecha,
        estacion_id::varchar                 as estacion_id,
        max(estacion_nombre)                 as estacion_nombre,
        avg(latitud)                         as latitud,
        avg(longitud)                        as longitud,
        'pm25'                               as parametro,
        round(avg(valor_promedio), 2)        as pm25_promedio_24h,
        round(avg(cobertura_pct), 1)         as cobertura_pct,
        count(distinct sensor_id)            as sensores,
        max(fuente)                          as fuente,
        max(_cargado_en)                     as cargado_en
    from origen
    where valor_promedio is not null
    group by 1, 2

)

select * from por_estacion
