-- fct_salud_aire_distrito_dia: UNA fila por distrito y dia, con atenciones
-- respiratorias, PM2.5 de la estacion de referencia, categoria de calidad del
-- aire, comparacion contra los 7 dias previos y bandera de alerta.
--
-- Regla de alerta de salud publica:
--   aire por encima de la guia OMS (PM2.5 24 h > guia_oms_pm25_24h)
--   Y atenciones al menos umbral_alza_atenciones_pct % sobre el promedio de
--   los 7 dias previos del mismo distrito.

with distritos as (

    select * from {{ ref('dim_distrito') }}

),

atenciones as (

    select * from {{ ref('int_atenciones_distrito_dia') }}

),

pm25 as (

    select * from {{ ref('int_pm25_distrito_dia') }}

),

calendario as (

    select fecha from atenciones
    union
    select fecha from pm25

),

-- Rejilla completa dia x distrito: un dia sin atenciones en un distrito es un 0,
-- no una fila faltante (si no, el promedio movil de 7 dias se distorsiona).
base as (

    select c.fecha, d.ubigeo, d.distrito, d.zona
    from calendario as c
    cross join distritos as d

),

unido as (

    select
        b.fecha,
        b.ubigeo,
        b.distrito,
        b.zona,
        coalesce(a.total_atenciones, 0)             as total_atenciones,
        coalesce(a.atenciones_menores_5, 0)         as atenciones_menores_5,
        coalesce(a.atenciones_adultos_mayores, 0)   as atenciones_adultos_mayores,
        coalesce(a.atenciones_asma, 0)              as atenciones_asma,
        coalesce(a.atenciones_neumonia, 0)          as atenciones_neumonia,
        coalesce(a.clinicas_reportantes, 0)         as clinicas_reportantes,
        p.pm25_promedio_24h,
        p.estacion_referencia,
        p.distancia_estacion_km,
        p.fuente_calidad_aire
    from base as b
    left join atenciones as a
        on b.fecha = a.fecha and b.ubigeo = a.ubigeo
    left join pm25 as p
        on b.fecha = p.fecha and b.ubigeo = p.ubigeo

),

con_indicadores as (

    select
        *,
        case
            when pm25_promedio_24h is null                              then 'SIN_COBERTURA'
            when pm25_promedio_24h <= {{ var('guia_oms_pm25_24h') }}    then 'DENTRO_GUIA_OMS'
            when pm25_promedio_24h <= {{ var('eca_peru_pm25_24h') }}    then 'SOBRE_GUIA_OMS'
            else 'SOBRE_ECA_PERU'
        end as categoria_aire,
        avg(total_atenciones) over (
            partition by ubigeo order by fecha
            rows between 7 preceding and 1 preceding
        ) as promedio_atenciones_7d_previos
    from unido

)

select
    fecha,
    ubigeo,
    distrito,
    zona,
    total_atenciones,
    atenciones_menores_5,
    atenciones_adultos_mayores,
    atenciones_asma,
    atenciones_neumonia,
    clinicas_reportantes,
    pm25_promedio_24h,
    categoria_aire,
    estacion_referencia,
    distancia_estacion_km,
    fuente_calidad_aire,
    round(promedio_atenciones_7d_previos, 1) as promedio_atenciones_7d_previos,
    case
        when promedio_atenciones_7d_previos > 0
            then round((total_atenciones - promedio_atenciones_7d_previos)
                       / promedio_atenciones_7d_previos * 100, 1)
    end as variacion_vs_7d_pct,
    coalesce(
        categoria_aire in ('SOBRE_GUIA_OMS', 'SOBRE_ECA_PERU')
        and promedio_atenciones_7d_previos > 0
        and (total_atenciones - promedio_atenciones_7d_previos) / promedio_atenciones_7d_previos * 100
            >= {{ var('umbral_alza_atenciones_pct') }},
        false
    ) as alerta_salud_publica,
    current_timestamp() as actualizado_en
from con_indicadores
