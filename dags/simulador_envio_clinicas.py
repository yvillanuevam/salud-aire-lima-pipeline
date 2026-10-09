"""
### simulador_envio_clinicas — SIMULA al proveedor externo (no es parte del pipeline)

En la vida real, la red de clinicas sube cada madrugada su archivo de
atenciones respiratorias del dia anterior al servidor SFTP. Como no tenemos
acceso a ese sistema, este DAG hace ese papel: genera el CSV (con los defectos
tipicos de un export de Excel: separador `;`, codificacion Windows-1252,
fechas dd/mm/aaaa, duplicados, celdas "N/D", distritos escritos de varias
formas) y lo sube al SFTP con la misma Connection que usa el pipeline.

Corre a las 05:30, antes del `pipeline_salud_aire` (06:00), cuyo sensor espera
este archivo. Para la demo se puede disparar a mano con
`{"fecha_proceso": "AAAA-MM-DD"}`.

En produccion este DAG se elimina: el enunciado (seccion 12) permite simular
la fuente siempre que el codigo de ingesta sea el de una fuente real.
"""

from __future__ import annotations

import logging
import tempfile
from datetime import date, timedelta
from pathlib import Path

import pendulum
from airflow.sdk import Param, dag, get_current_context, task

from salud_aire import config
from salud_aire.alertas import notificar_fallo

log = logging.getLogger(__name__)


@dag(
    dag_id="simulador_envio_clinicas",
    description="Simula a la red de clinicas subiendo su CSV diario al SFTP",
    schedule="30 5 * * *",
    start_date=pendulum.datetime(2026, 10, 1, tz=config.ZONA_HORARIA),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "simulacion_proveedor",
        "retries": 2,
        "retry_delay": timedelta(minutes=1),
        "on_failure_callback": notificar_fallo,
    },
    params={
        "fecha_proceso": Param(
            None, type=["null", "string"], format="date",
            description="Opcional (AAAA-MM-DD). Vacio = dia anterior.",
        )
    },
    tags=["salud_aire", "simulador", "sftp"],
    doc_md=__doc__,
)
def simulador_envio_clinicas():

    @task
    def generar_y_subir_archivo() -> str:
        from airflow.providers.sftp.hooks.sftp import SFTPHook

        from salud_aire.fechas import resolver_fecha_proceso
        from salud_aire.simulacion import generar_csv_atenciones

        contexto = get_current_context()
        fecha: date = resolver_fecha_proceso(
            contexto["params"].get("fecha_proceso"), contexto.get("data_interval_end")
        )
        contenido = generar_csv_atenciones(fecha)
        ruta_remota = config.ruta_sftp_atenciones(fecha.isoformat())

        hook = SFTPHook(ssh_conn_id=config.CONN_SFTP)
        hook.create_directory(config.CARPETA_SFTP_ATENCIONES)
        with tempfile.TemporaryDirectory() as tmp:
            ruta_local = Path(tmp) / Path(ruta_remota).name
            ruta_local.write_bytes(contenido)
            hook.store_file(ruta_remota, str(ruta_local))
        log.info("Archivo simulado subido a sftp://%s (%s bytes)", ruta_remota, len(contenido))
        return ruta_remota

    generar_y_subir_archivo()


simulador_envio_clinicas()
