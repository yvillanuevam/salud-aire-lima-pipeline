-- Consultas de ejemplo de la capa de consumo (para Snowsight o un dashboard).
-- Ejecutar con el rol de solo lectura: USE ROLE ROLE_SALUD_AIRE_LECTOR;

-- 1) ¿Que distritos estan en alerta en el ultimo dia procesado?
select fecha, distrito, zona, total_atenciones, pm25_promedio_24h,
       categoria_aire, variacion_vs_7d_pct
from SALUD_AIRE_DB.MARTS.FCT_SALUD_AIRE_DISTRITO_DIA
where fecha = (select max(fecha) from SALUD_AIRE_DB.MARTS.FCT_SALUD_AIRE_DISTRITO_DIA)
  and alerta_salud_publica
order by pm25_promedio_24h desc;

-- 2) Atenciones promedio por categoria de calidad del aire (ultimos 30 dias):
--    ¿hay mas atenciones los dias con peor aire?
select categoria_aire,
       count(*)                         as distrito_dias,
       round(avg(total_atenciones), 1)  as atenciones_promedio,
       round(avg(atenciones_menores_5), 1) as menores_5_promedio
from SALUD_AIRE_DB.MARTS.FCT_SALUD_AIRE_DISTRITO_DIA
where fecha >= dateadd(day, -30, current_date())
group by categoria_aire
order by atenciones_promedio desc;

-- 3) Ranking de zonas por dias sobre la guia OMS en el mes.
select zona,
       count_if(categoria_aire in ('SOBRE_GUIA_OMS', 'SOBRE_ECA_PERU')) as dias_sobre_oms,
       sum(total_atenciones) as atenciones
from SALUD_AIRE_DB.MARTS.FCT_SALUD_AIRE_DISTRITO_DIA
where date_trunc('month', fecha) = date_trunc('month', current_date())
group by zona
order by dias_sobre_oms desc;
