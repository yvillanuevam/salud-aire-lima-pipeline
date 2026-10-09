"""Tests de la carga idempotente a Snowflake, del reporte y de las fechas."""

from datetime import date, datetime
from unittest.mock import MagicMock
from zoneinfo import ZoneInfo

import pytest

from salud_aire.carga_snowflake import reemplazar_particion
from salud_aire.config import clave_minio
from salud_aire.fechas import resolver_fecha_proceso
from salud_aire.reporte import construir_reporte

LIMA = ZoneInfo("America/Lima")


# ---------------------------------------------------------------- fechas
def test_corrida_programada_procesa_el_dia_anterior_en_hora_de_lima():
    # 06:00 de Lima del 8 de octubre = 11:00 UTC
    fin_intervalo = datetime(2026, 10, 8, 11, 0, tzinfo=ZoneInfo("UTC"))
    assert resolver_fecha_proceso(None, fin_intervalo) == date(2026, 10, 7)


def test_cerca_de_medianoche_utc_se_usa_la_fecha_de_lima_no_la_utc():
    # 02:00 UTC del 9 = 21:00 de Lima del 8 -> se procesa el 7
    assert resolver_fecha_proceso(None, datetime(2026, 10, 9, 2, 0, tzinfo=ZoneInfo("UTC"))) == date(2026, 10, 7)


def test_parametro_manual_tiene_prioridad_para_reprocesos():
    assert resolver_fecha_proceso("2026-09-15", datetime(2026, 10, 8, tzinfo=LIMA)) == date(2026, 9, 15)


def test_parametro_manual_mal_escrito_falla_con_mensaje_claro():
    with pytest.raises(ValueError, match="YYYY-MM-DD"):
        resolver_fecha_proceso("15/09/2026", None)


# ---------------------------------------------------------------- data lake
def test_clave_minio_particionada_por_fecha():
    assert clave_minio("raw", "atenciones", "2026-10-07", "a.csv") == "raw/atenciones/fecha=2026-10-07/a.csv"


def test_clave_minio_rechaza_capas_inexistentes():
    with pytest.raises(ValueError):
        clave_minio("temporal", "atenciones", "2026-10-07", "a.csv")


# ---------------------------------------------------------------- Snowflake
def _conexion_falsa():
    cursor = MagicMock()
    conexion = MagicMock()
    conexion.cursor.return_value = cursor
    return conexion, cursor


def test_reemplazar_particion_borra_el_dia_e_inserta_en_una_transaccion():
    conexion, cursor = _conexion_falsa()
    filas = [{"fecha": "2026-10-07", "valor": 1}, {"fecha": "2026-10-07", "valor": 2}]

    n = reemplazar_particion(conexion, "DB.RAW.T", "FECHA", "2026-10-07", ["FECHA", "VALOR"], filas)

    assert n == 2
    sentencias = [c.args[0] for c in cursor.execute.call_args_list]
    assert sentencias == ["BEGIN", "DELETE FROM DB.RAW.T WHERE FECHA = %s", "COMMIT"]
    sql_insert, valores = cursor.executemany.call_args.args
    assert sql_insert == "INSERT INTO DB.RAW.T (FECHA, VALOR) VALUES (%s, %s)"
    assert valores == [("2026-10-07", 1), ("2026-10-07", 2)]


def test_reemplazar_particion_hace_rollback_si_falla_el_insert():
    conexion, cursor = _conexion_falsa()
    cursor.executemany.side_effect = RuntimeError("warehouse suspendido")

    with pytest.raises(RuntimeError):
        reemplazar_particion(conexion, "DB.RAW.T", "FECHA", "2026-10-07", ["FECHA"], [{"fecha": "2026-10-07"}])

    sentencias = [c.args[0] for c in cursor.execute.call_args_list]
    assert sentencias[-1] == "ROLLBACK"
    assert "COMMIT" not in sentencias


def test_reemplazar_particion_no_mezcla_dias():
    conexion, cursor = _conexion_falsa()
    with pytest.raises(ValueError, match="no pertenecen"):
        reemplazar_particion(conexion, "DB.RAW.T", "FECHA", "2026-10-07", ["FECHA"], [{"fecha": "2026-10-06"}])
    cursor.execute.assert_not_called()


@pytest.mark.parametrize("tabla", ["RAW.T; DROP TABLE X", "RAW.T --", "1TABLA"])
def test_reemplazar_particion_rechaza_identificadores_peligrosos(tabla):
    conexion, _ = _conexion_falsa()
    with pytest.raises(ValueError, match="Identificador"):
        reemplazar_particion(conexion, tabla, "FECHA", "2026-10-07", ["FECHA"], [])


# ---------------------------------------------------------------- reporte
def _fila(distrito, pm25, alerta, total=10):
    return {
        "distrito": distrito, "zona": "Lima Este", "total_atenciones": total,
        "atenciones_menores_5": 2, "atenciones_adultos_mayores": 1,
        "pm25_promedio_24h": pm25, "categoria_aire": "SOBRE_GUIA_OMS" if pm25 and pm25 > 15 else "DENTRO_GUIA_OMS",
        "variacion_vs_7d_pct": 25.0 if alerta else None, "alerta_salud_publica": alerta,
    }


def test_reporte_resume_y_ordena_alertas_primero():
    filas = [_fila("San Borja", 12.0, False), _fila("Ate", 38.5, True, total=40), _fila("Comas", None, False)]

    reporte = construir_reporte("2026-10-07", filas)

    assert "Atenciones respiratorias reportadas: **60**" in reporte
    assert "Mayor PM2.5 del dia: **38.5 µg/m³** en Ate" in reporte
    assert "Distritos en ALERTA de salud publica: **1**" in reporte
    tabla = [linea for linea in reporte.splitlines() if linea.startswith("| ") and "Distrito" not in linea]
    assert tabla[0].startswith("| Ate |")  # la alerta va primero
    assert "+25.0%" in tabla[0]


def test_reporte_sin_filas_falla_en_vez_de_publicar_un_reporte_vacio():
    with pytest.raises(ValueError, match="no tiene filas"):
        construir_reporte("2026-10-07", [])
