-- int_atenciones_distrito_dia: atenciones agregadas por distrito oficial (ubigeo) y dia.
-- Aqui se resuelve el texto libre del distrito contra el diccionario de alias.
-- Las filas cuyo distrito no se reconoce (p. ej. CALLAO, fuera de Lima
-- Metropolitana) quedan fuera y se reportan en AUDITORIA.DISTRITOS_SIN_MAPEAR.

with atenciones as (

    select * from {{ ref('stg_atenciones') }}

),

alias as (

    select * from {{ ref('alias_distritos') }}

),

mapeadas as (

    select
        a.fecha_atencion,
        al.ubigeo,
        a.codigo_clinica,
        a.grupo_edad,
        a.categoria_cie10,
        a.num_atenciones
    from atenciones as a
    inner join alias as al
        on a.distrito_reportado = al.alias_normalizado

)

select
    fecha_atencion                                                    as fecha,
    ubigeo,
    sum(num_atenciones)                                               as total_atenciones,
    sum(iff(grupo_edad = '0-4', num_atenciones, 0))                   as atenciones_menores_5,
    sum(iff(grupo_edad = '60+', num_atenciones, 0))                   as atenciones_adultos_mayores,
    sum(iff(categoria_cie10 = 'J45', num_atenciones, 0))              as atenciones_asma,
    sum(iff(categoria_cie10 in ('J12', 'J13', 'J14', 'J15', 'J16', 'J17', 'J18'),
            num_atenciones, 0))                                       as atenciones_neumonia,
    count(distinct codigo_clinica)                                    as clinicas_reportantes
from mapeadas
group by 1, 2
