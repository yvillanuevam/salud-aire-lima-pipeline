-- stg_atenciones: limpieza, tipado y deduplicacion de las atenciones crudas.
-- Regla de negocio: si una clinica reenvia la misma combinacion
-- (fecha, clinica, distrito, grupo de edad, diagnostico), vale la ULTIMA
-- fila del archivo (es una correccion o un reenvio).

with origen as (

    select * from {{ source('raw', 'atenciones_respiratorias') }}

),

limpio as (

    select
        fecha_atencion::date                          as fecha_atencion,
        upper(trim(codigo_clinica))                   as codigo_clinica,
        {{ normalizar_texto('distrito') }}            as distrito_reportado,
        replace(grupo_edad, ' ', '')                  as grupo_edad,
        upper(trim(diagnostico_cie10))                as diagnostico_cie10,
        left(upper(trim(diagnostico_cie10)), 3)       as categoria_cie10,
        num_atenciones::integer                       as num_atenciones,
        _numero_fila                                  as numero_fila,
        _archivo_origen                               as archivo_origen,
        _cargado_en                                   as cargado_en
    from origen

)

select *
from limpio
qualify row_number() over (
    partition by fecha_atencion, codigo_clinica, distrito_reportado, grupo_edad, diagnostico_cie10
    order by cargado_en desc, numero_fila desc
) = 1
