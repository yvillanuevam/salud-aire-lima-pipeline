"""Cliente de la API publica OpenAQ v3 (fuente 2: calidad del aire).

La API es "imperfecta" en el sentido del curso: exige API key, tiene limite
de uso (plan gratuito: 60 req/min y 2000 req/h) y responde 429 cuando se
excede. El cliente:
  * reintenta 429 y errores 5xx respetando Retry-After / x-ratelimit-reset,
    con backoff exponencial acotado;
  * NO reintenta 401/403 (API key invalida): falla de inmediato con un
    mensaje claro, porque reintentar no lo va a arreglar;
  * al agotar los intentos lanza una excepcion explicita para que Airflow
    aplique sus propios retries (con retry_exponential_backoff).

Documentacion: https://docs.openaq.org
"""

from __future__ import annotations

import logging
import time
from collections.abc import Callable, Mapping
from datetime import date, timedelta
from typing import Any

import requests

log = logging.getLogger(__name__)

ESTADOS_REINTENTABLES = {429, 500, 502, 503, 504}


class ErrorApiCalidadAire(RuntimeError):
    """Falla no recuperable (o recuperable pero agotada) al consultar la API."""


def segundos_espera(
    intento: int,
    cabeceras: Mapping[str, str] | None = None,
    base: float = 2.0,
    maximo: float = 60.0,
) -> float:
    """Cuanto esperar antes del siguiente intento.

    Si la API indica cuando reintentar (Retry-After o x-ratelimit-reset, en
    segundos) se respeta; si no, backoff exponencial: base * 2**intento.
    Siempre entre 1 segundo y ``maximo``.
    """
    cabeceras = {k.lower(): v for k, v in (cabeceras or {}).items()}
    for nombre in ("retry-after", "x-ratelimit-reset"):
        valor = cabeceras.get(nombre)
        if valor is None:
            continue
        try:
            return min(max(float(valor), 1.0), maximo)
        except ValueError:
            continue  # p. ej. una fecha HTTP: se ignora y se usa el backoff
    return min(max(base * (2**intento), 1.0), maximo)


class ClienteOpenAQ:
    def __init__(
        self,
        api_key: str,
        url_base: str = "https://api.openaq.org",
        max_intentos: int = 4,
        timeout_s: float = 30.0,
        sesion: requests.Session | None = None,
        dormir: Callable[[float], None] = time.sleep,
    ) -> None:
        if not api_key:
            raise ErrorApiCalidadAire(
                "No hay API key de OpenAQ. Definela en .env (OPENAQ_API_KEY) para la "
                "Connection 'openaq_api', o usa FUENTE_CALIDAD_AIRE=simulada."
            )
        self._api_key = api_key
        self.url_base = url_base.rstrip("/")
        self.max_intentos = max_intentos
        self.timeout_s = timeout_s
        self.sesion = sesion or requests.Session()
        self._dormir = dormir

    def _get(self, ruta: str, params: Mapping[str, Any] | None = None) -> dict:
        url = f"{self.url_base}{ruta}"
        cabeceras = {"X-API-Key": self._api_key, "Accept": "application/json"}
        ultimo_error = ""
        for intento in range(self.max_intentos):
            try:
                resp = self.sesion.get(url, params=params, headers=cabeceras, timeout=self.timeout_s)
            except (requests.ConnectionError, requests.Timeout) as exc:
                ultimo_error = f"error de red: {exc}"
                espera = segundos_espera(intento)
            else:
                if resp.status_code == 200:
                    return resp.json()
                if resp.status_code in (401, 403):
                    raise ErrorApiCalidadAire(
                        f"OpenAQ rechazo la API key (HTTP {resp.status_code}). Revisa OPENAQ_API_KEY."
                    )
                if resp.status_code not in ESTADOS_REINTENTABLES:
                    raise ErrorApiCalidadAire(
                        f"OpenAQ respondio HTTP {resp.status_code} en {ruta}: {resp.text[:200]}"
                    )
                ultimo_error = f"HTTP {resp.status_code}"
                espera = segundos_espera(intento, resp.headers)

            if intento < self.max_intentos - 1:
                log.warning(
                    "OpenAQ %s (intento %s/%s). Reintento en %.0f s.",
                    ultimo_error, intento + 1, self.max_intentos, espera,
                )
                self._dormir(espera)

        raise ErrorApiCalidadAire(
            f"OpenAQ no respondio tras {self.max_intentos} intentos ({ultimo_error}) en {ruta}"
        )

    def listar_ubicaciones(
        self, latitud: float, longitud: float, radio_m: int, parametro_id: int, limite: int = 100
    ) -> list[dict]:
        datos = self._get(
            "/v3/locations",
            {
                "coordinates": f"{latitud:.4f},{longitud:.4f}",
                "radius": radio_m,
                "parameters_id": parametro_id,
                "limit": limite,
            },
        )
        return datos.get("results", [])

    def promedios_diarios_sensor(self, sensor_id: int | str, fecha: date) -> list[dict]:
        # Se pide una ventana de +-1 dia y luego se elige el dia local exacto,
        # porque la API agrega por dia en hora local de la estacion.
        datos = self._get(
            f"/v3/sensors/{sensor_id}/days",
            {
                "date_from": (fecha - timedelta(days=1)).isoformat(),
                "date_to": (fecha + timedelta(days=1)).isoformat(),
                "limit": 10,
            },
        )
        return datos.get("results", [])


def seleccionar_sensores_pm25(
    ubicaciones: list[dict], fecha: date, max_sensores: int
) -> list[dict]:
    """De la respuesta de /v3/locations, elige los sensores PM2.5 utiles.

    Descarta estaciones moviles y sensores sin datos desde antes de ``fecha``
    (estaciones apagadas): asi no se gastan llamadas a la API en vano.
    """
    sensores: list[dict] = []
    for ubicacion in ubicaciones:
        if ubicacion.get("isMobile"):
            continue
        ultimo = ((ubicacion.get("datetimeLast") or {}).get("utc") or "")[:10]
        if not ultimo or ultimo < fecha.isoformat():
            continue
        coordenadas = ubicacion.get("coordinates") or {}
        for sensor in ubicacion.get("sensors", []):
            if (sensor.get("parameter") or {}).get("name") != "pm25":
                continue
            sensores.append(
                {
                    "estacion_id": str(ubicacion["id"]),
                    "estacion_nombre": ubicacion.get("name"),
                    "latitud": coordenadas.get("latitude"),
                    "longitud": coordenadas.get("longitude"),
                    "sensor_id": str(sensor["id"]),
                }
            )
    sensores.sort(key=lambda s: (s["estacion_id"], s["sensor_id"]))
    return sensores[:max_sensores]


def parsear_promedio_diario(resultados: list[dict], sensor: dict, fecha: date) -> dict | None:
    """Convierte la respuesta de /v3/sensors/{id}/days en una fila para RAW.

    Devuelve None si la API no tiene el promedio de ese dia (es normal: hay
    estaciones que se caen) o si el valor viene vacio.
    """
    for item in resultados:
        periodo = item.get("period") or {}
        inicio_local = ((periodo.get("datetimeFrom") or {}).get("local") or "")[:10]
        if inicio_local != fecha.isoformat():
            continue
        if item.get("value") is None:
            return None
        parametro = item.get("parameter") or {}
        cobertura = item.get("coverage") or {}
        return {
            "fecha": fecha.isoformat(),
            "estacion_id": sensor["estacion_id"],
            "estacion_nombre": sensor.get("estacion_nombre"),
            "latitud": sensor.get("latitud"),
            "longitud": sensor.get("longitud"),
            "sensor_id": sensor["sensor_id"],
            "parametro": parametro.get("name", "pm25"),
            "unidad": parametro.get("units"),
            "valor_promedio": round(float(item["value"]), 3),
            "cobertura_pct": cobertura.get("percentComplete"),
            "n_observaciones": cobertura.get("observedCount"),
            "fuente": "openaq",
        }
    return None
