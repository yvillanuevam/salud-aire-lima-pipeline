"""El simulador del proveedor debe producir un archivo que el pipeline real
sepa procesar, con los defectos esperados (y ninguno inesperado)."""

from datetime import date

from salud_aire.atenciones import validar_atenciones
from salud_aire.simulacion import (
    factor_contaminacion_dia,
    generar_csv_atenciones,
    medicion_simulada,
    sensores_simulados,
)

FECHA = date(2026, 10, 7)


def test_simulacion_es_deterministica_por_fecha():
    assert generar_csv_atenciones(FECHA) == generar_csv_atenciones(FECHA)
    assert generar_csv_atenciones(FECHA) != generar_csv_atenciones(date(2026, 10, 8))


def test_archivo_simulado_pasa_la_validacion_con_pocas_filas_en_cuarentena():
    resultado = validar_atenciones(generar_csv_atenciones(FECHA), FECHA)

    assert resultado.codificacion == "cp1252" and resultado.separador == ";"
    assert len(resultado.validas) > 100
    # Defectos sembrados a proposito: N/D, negativo y codigo no respiratorio
    # (pueden coincidir en la misma fila, por eso "entre 1 y 3").
    assert 1 <= len(resultado.rechazadas) <= 3
    assert resultado.tasa_rechazo < 0.05


def test_mediciones_simuladas_estan_marcadas_y_en_rango():
    filas = [medicion_simulada(s, FECHA) for s in sensores_simulados()]
    validas = [f for f in filas if f]

    assert validas, "al menos una estacion debe reportar"
    assert all(f["fuente"] == "simulada" and f["estacion_id"].startswith("SIM-") for f in validas)
    assert all(0 < f["valor_promedio"] < 200 for f in validas)


def test_factor_del_dia_en_rango_esperado():
    assert all(0.6 <= factor_contaminacion_dia(date(2026, 1, d)) <= 1.7 for d in range(1, 32))
