-- Test singular de conciliacion (severity error): por cada dia, el total de
-- atenciones del mart debe ser EXACTAMENTE la suma de las atenciones de
-- staging cuyo distrito se pudo mapear. Si no cuadra, algun join esta
-- perdiendo o duplicando filas: se detiene el pipeline antes del reporte.
{{ config(severity='error') }}

with staging as (

    select s.fecha_atencion as fecha, sum(s.num_atenciones) as total_staging
    from {{ ref('stg_atenciones') }} as s
    inner join {{ ref('alias_distritos') }} as a
        on s.distrito_reportado = a.alias_normalizado
    group by 1

),

mart as (

    select fecha, sum(total_atenciones) as total_mart
    from {{ ref('fct_salud_aire_distrito_dia') }}
    group by 1

)

select
    coalesce(s.fecha, m.fecha)            as fecha,
    coalesce(s.total_staging, 0)          as total_staging,
    coalesce(m.total_mart, 0)             as total_mart
from staging as s
full outer join mart as m
    on s.fecha = m.fecha
where coalesce(s.total_staging, 0) <> coalesce(m.total_mart, 0)
