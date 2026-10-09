"""Tests del cliente OpenAQ: parseo de respuestas y manejo del limite de uso.

La API real se reemplaza con mocks: los tests son rapidos, deterministicos y
no gastan la cuota de la API key (ni necesitan una en GitHub Actions).
"""

import json
from datetime import date
from pathlib import Path
from unittest.mock import MagicMock

import pytest
import requests

from salud_aire.calidad_aire import (
    ClienteOpenAQ,
    ErrorApiCalidadAire,
    parsear_promedio_diario,
    segundos_espera,
    seleccionar_sensores_pm25,
)

FIXTURES = Path(__file__).parent / "fixtures"
FECHA = date(2026, 10, 7)


def _respuesta(status: int, cuerpo: dict | None = None, cabeceras: dict | None = None) -> MagicMock:
    resp = MagicMock()
    resp.status_code = status
    resp.json.return_value = cuerpo or {}
    resp.headers = cabeceras or {}
    resp.text = json.dumps(cuerpo or {})
    return resp


def _cliente(*respuestas) -> tuple[ClienteOpenAQ, MagicMock, list[float]]:
    sesion = MagicMock()
    sesion.get.side_effect = list(respuestas)
    esperas: list[float] = []
    cliente = ClienteOpenAQ(api_key="clave-de-prueba", sesion=sesion, dormir=esperas.append)
    return cliente, sesion, esperas


# ---------------------------------------------------------------- backoff
def test_segundos_espera_respeta_retry_after_de_la_api():
    assert segundos_espera(0, {"Retry-After": "7"}) == 7.0


def test_segundos_espera_usa_x_ratelimit_reset_si_no_hay_retry_after():
    assert segundos_espera(0, {"x-ratelimit-reset": "12"}) == 12.0


def test_segundos_espera_backoff_exponencial_acotado():
    assert [segundos_espera(i) for i in range(4)] == [2.0, 4.0, 8.0, 16.0]
    assert segundos_espera(10) == 60.0  # nunca mas del maximo


# ---------------------------------------------------------------- cliente HTTP
def test_cliente_reintenta_ante_429_y_luego_devuelve_los_datos():
    datos = {"results": [{"id": 1}]}
    cliente, sesion, esperas = _cliente(
        _respuesta(429, cabeceras={"Retry-After": "3"}),
        _respuesta(200, datos),
    )

    assert cliente.listar_ubicaciones(-12.04, -77.04, 25000, 2) == [{"id": 1}]
    assert sesion.get.call_count == 2
    assert esperas == [3.0]
    # la API key viaja en la cabecera, nunca en la URL
    assert sesion.get.call_args.kwargs["headers"]["X-API-Key"] == "clave-de-prueba"


def test_cliente_reintenta_errores_de_red():
    cliente, sesion, esperas = _cliente(requests.ConnectionError("sin red"), _respuesta(200, {"results": []}))

    assert cliente.listar_ubicaciones(-12.04, -77.04, 25000, 2) == []
    assert esperas == [2.0]


def test_cliente_no_reintenta_api_key_invalida():
    cliente, sesion, _ = _cliente(_respuesta(401), _respuesta(200))

    with pytest.raises(ErrorApiCalidadAire, match="API key"):
        cliente.listar_ubicaciones(-12.04, -77.04, 25000, 2)
    assert sesion.get.call_count == 1


def test_cliente_falla_explicitamente_al_agotar_los_intentos():
    cliente, sesion, esperas = _cliente(*[_respuesta(503)] * 4)

    with pytest.raises(ErrorApiCalidadAire, match="tras 4 intentos"):
        cliente.listar_ubicaciones(-12.04, -77.04, 25000, 2)
    assert len(esperas) == 3  # no se duerme despues del ultimo intento


def test_cliente_sin_api_key_falla_con_instrucciones():
    with pytest.raises(ErrorApiCalidadAire, match="OPENAQ_API_KEY"):
        ClienteOpenAQ(api_key="")


# ---------------------------------------------------------------- parseo
def test_seleccionar_sensores_descarta_moviles_apagados_y_otros_contaminantes():
    ubicaciones = json.loads((FIXTURES / "openaq_locations.json").read_text(encoding="utf-8"))["results"]

    sensores = seleccionar_sensores_pm25(ubicaciones, FECHA, max_sensores=20)

    assert sensores == [
        {
            "estacion_id": "101",
            "estacion_nombre": "Estacion fija activa",
            "latitud": -12.0262,
            "longitud": -76.919,
            "sensor_id": "5001",
        }
    ]


def test_parsear_promedio_diario_elige_el_dia_local_correcto():
    resultados = json.loads((FIXTURES / "openaq_sensor_days.json").read_text(encoding="utf-8"))["results"]
    sensor = {"estacion_id": "101", "estacion_nombre": "X", "latitud": -12.0, "longitud": -77.0, "sensor_id": "5001"}

    fila = parsear_promedio_diario(resultados, sensor, FECHA)

    assert fila["fecha"] == "2026-10-07"
    assert fila["valor_promedio"] == 28.746
    assert fila["cobertura_pct"] == 75.0
    assert fila["n_observaciones"] == 18
    assert fila["fuente"] == "openaq"


def test_parsear_promedio_diario_sin_dato_devuelve_none():
    sensor = {"estacion_id": "1", "sensor_id": "1"}
    assert parsear_promedio_diario([], sensor, FECHA) is None
