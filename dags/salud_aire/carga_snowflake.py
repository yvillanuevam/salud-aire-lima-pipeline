"""Carga idempotente a las tablas RAW de Snowflake.

Patron "reemplazar particion": dentro de UNA transaccion se borran las filas
del dia procesado y se insertan las nuevas. Si la tarea se reintenta o se
reprocesa el mismo dia, el resultado es el mismo (no se duplican datos), y si
algo falla a mitad de camino se hace ROLLBACK y la tabla queda como estaba.
"""

from __future__ import annotations

import re
from typing import Any, Protocol

_IDENTIFICADOR = re.compile(r"^[A-Za-z_][A-Za-z0-9_$]*(\.[A-Za-z_][A-Za-z0-9_$]*){0,2}$")


class ConexionDbApi(Protocol):  # lo que expone snowflake.connector y cualquier DB-API 2.0
    def cursor(self) -> Any: ...


def _validar_identificador(nombre: str) -> str:
    """Los nombres de tabla/columna no pueden ir como parametros SQL; por eso
    se validan contra un patron estricto (defensa contra inyeccion SQL)."""
    if not _IDENTIFICADOR.match(nombre):
        raise ValueError(f"Identificador SQL invalido: {nombre!r}")
    return nombre


def reemplazar_particion(
    conexion: ConexionDbApi,
    tabla: str,
    columna_fecha: str,
    fecha_iso: str,
    columnas: list[str],
    filas: list[dict],
) -> int:
    """DELETE del dia + INSERT de las filas nuevas, de forma atomica.

    Devuelve la cantidad de filas insertadas.
    """
    _validar_identificador(tabla)
    _validar_identificador(columna_fecha)
    for columna in columnas:
        _validar_identificador(columna)

    fuera_de_fecha = [f for f in filas if str(f.get(columna_fecha.lower(), fecha_iso)) != fecha_iso]
    if fuera_de_fecha:
        raise ValueError(
            f"{len(fuera_de_fecha)} filas no pertenecen a {fecha_iso}; se aborta para no "
            "mezclar particiones."
        )

    marcadores = ", ".join(["%s"] * len(columnas))
    sql_insert = f"INSERT INTO {tabla} ({', '.join(columnas)}) VALUES ({marcadores})"
    valores = [tuple(fila.get(c.lower()) for c in columnas) for fila in filas]

    cursor = conexion.cursor()
    try:
        cursor.execute("BEGIN")
        cursor.execute(f"DELETE FROM {tabla} WHERE {columna_fecha} = %s", (fecha_iso,))
        if valores:
            cursor.executemany(sql_insert, valores)
        cursor.execute("COMMIT")
    except Exception:
        cursor.execute("ROLLBACK")
        raise
    finally:
        cursor.close()
    return len(valores)
