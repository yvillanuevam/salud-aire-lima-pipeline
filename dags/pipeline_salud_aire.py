"""
### pipeline_salud_aire — Calidad del aire vs. atenciones respiratorias en Lima

**Pregunta de negocio:** ¿en que distritos de Lima Metropolitana la mala calidad
del aire (PM2.5) coincide con un aumento de atenciones por enfermedades
respiratorias? El area de salud publica usa el resultado para reforzar
personal y campañas preventivas en los distritos en alerta.

**Que hace, cada dia a las 06:00 (hora de Lima), para el dia anterior:**

1. **Ingesta**
   - *Atenciones (SFTP):* un sensor (`mode="reschedule"`) espera el CSV que la red
     de clinicas deja en el SFTP; se valida (encabezados, tipos, fechas, codigos
     CIE-10) y las filas invalidas van a cuarentena con su motivo.
   - *Calidad del aire (API OpenAQ v3):* se buscan los sensores PM2.5 a 25 km del
     centro de Lima y se consulta el promedio diario de cada uno con
     Dynamic Task Mapping, limitado por el pool `api_openaq` (la API tiene
     limite de uso) y con reintentos con backoff ante HTTP 429/5xx.
2. **Staging intermedio:** todo pasa por MinIO (`raw/`, `staged/`, `cuarentena/`).
3. **Carga:** de MinIO a `SALUD_AIRE_DB.RAW` en Snowflake (DELETE + INSERT del dia
   en una transaccion: idempotente).
4. **Transformacion:** dbt orquestado con Cosmos (`staging -> intermediate -> marts`),
   con tests de dbt despues de cada modelo.
5. **Consumo:** reporte Markdown/CSV del dia en `reportes/` de MinIO.

**Reprocesar un dia:** *Trigger DAG w/ config* con `{"fecha_proceso": "AAAA-MM-DD"}`.

**Responsables:** `ingenieria_datos` (ingesta y carga) y `analytics_salud`
(modelos dbt). Connections usadas: `sftp_clinicas`, `openaq_api`,
`minio_datalake`, `snowflake_salud_aire` (credenciales solo en `.env`).
"""

from __future__ import annotations

import json
import logging
import os
import tempfile
from datetime import date, timedelta
from pathlib import Path

import pendulum
from airflow.providers.sftp.sensors.sftp import SFTPSensor
from airflow.sdk import Param, dag, get_current_context, task, task_group
from cosmos import DbtTaskGroup, ExecutionConfig, ProfileConfig, ProjectConfig, RenderConfig
from cosmos.constants import InvocationMode, LoadMode, TestBehavior

from salud_aire import config
from salud_aire.alertas import notificar_fallo

log = logging.getLogger(__name__)

DBT_PROJECT_DIR = Path(__file__).parent / "dbt" / "salud_aire"
DBT_EXECUTABLE_PATH = os.environ.get("DBT_EXECUTABLE_PATH", "/opt/airflow/dbt_venv/bin/dbt")

COLUMNAS_RAW_ATENCIONES = [
    "FECHA_ATENCION", "CODIGO_CLINICA", "DISTRITO", "GRUPO_EDAD",
    "DIAGNOSTICO_CIE10", "NUM_ATENCIONES", "_NUMERO_FILA", "_ARCHIVO_ORIGEN",
]
COLUMNAS_RAW_MEDICIONES = [
    "FECHA", "ESTACION_ID", "ESTACION_NOMBRE", "LATITUD", "LONGITUD", "SENSOR_ID",
    "PARAMETRO", "UNIDAD", "VALOR_PROMEDIO", "COBERTURA_PCT", "N_OBSERVACIONES",
    "FUENTE", "_ARCHIVO_ORIGEN",
]

DEFAULT_ARGS = {
    "owner": "ingenieria_datos",
    # Fallas transitorias (red, SFTP ocupado, warehouse reanudandose) se
    # resuelven solas en minutos: 2 reintentos con backoff exponencial.
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=20),
    "execution_timeout": timedelta(minutes=30),
    "on_failure_callback": notificar_fallo,
}


def _bucket() -> str:
    from airflow.sdk import Variable

    return Variable.get(config.VAR_BUCKET, default=config.BUCKET_POR_DEFECTO)


def _s3_hook():
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook

    return S3Hook(aws_conn_id=config.CONN_MINIO)


@dag(
    dag_id="pipeline_salud_aire",
    description="SFTP + API OpenAQ -> MinIO -> Snowflake -> dbt (Cosmos) -> reporte diario",
    schedule="0 6 * * *",
    start_date=pendulum.datetime(2026, 10, 1, tz=config.ZONA_HORARIA),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=5),
    default_args=DEFAULT_ARGS,
    params={
        "fecha_proceso": Param(
            None,
            type=["null", "string"],
            format="date",
            description="Opcional (AAAA-MM-DD). Vacio = dia anterior a la corrida.",
        )
    },
    tags=["salud_aire", "produccion", "sftp", "api", "minio", "snowflake", "dbt"],
    doc_md=__doc__,
)
def pipeline_salud_aire():

    @task(retries=1)
    def resolver_fecha() -> str:
        """Dia de negocio a procesar (por defecto, el dia anterior en hora de Lima)."""
        from salud_aire.fechas import resolver_fecha_proceso

        contexto = get_current_context()
        fecha = resolver_fecha_proceso(
            fecha_param=contexto["params"].get("fecha_proceso"),
            fin_intervalo=contexto.get("data_interval_end"),
        )
        log.info("Dia de negocio a procesar: %s", fecha)
        return fecha.isoformat()

    fecha = resolver_fecha()

    # ------------------------------------------------------------------
    # 1a) Fuente SFTP: atenciones de la red de clinicas
    # ------------------------------------------------------------------
    @task_group(group_id="ingesta_atenciones_sftp")
    def ingesta_atenciones(fecha_iso: str):
        esperar_archivo = SFTPSensor(
            task_id="esperar_archivo_atenciones",
            sftp_conn_id=config.CONN_SFTP,
            path=config.CARPETA_SFTP_ATENCIONES
            + "/atenciones_{{ ti.xcom_pull(task_ids='resolver_fecha') }}.csv",
            # reschedule: entre chequeos libera el slot del executor; el archivo
            # puede tardar horas y no queremos un slot bloqueado esperando.
            mode="reschedule",
            poke_interval=timedelta(minutes=5).total_seconds(),
            timeout=timedelta(hours=3).total_seconds(),  # SLA del proveedor: 09:00
            retries=1,
        )

        @task
        def transferir_atenciones_a_minio(fecha_iso: str) -> dict:
            """SFTP -> validacion -> MinIO (raw, staged y cuarentena)."""
            from airflow.providers.sftp.hooks.sftp import SFTPHook
            from airflow.sdk import Variable

            from salud_aire.atenciones import (
                COLUMNAS_CANONICAS,
                filas_a_csv,
                validar_atenciones,
                verificar_umbral_rechazo,
            )

            ruta_remota = config.ruta_sftp_atenciones(fecha_iso)
            with tempfile.TemporaryDirectory() as tmp:
                ruta_local = Path(tmp) / Path(ruta_remota).name
                SFTPHook(ssh_conn_id=config.CONN_SFTP).retrieve_file(ruta_remota, str(ruta_local))
                contenido = ruta_local.read_bytes()

            resultado = validar_atenciones(contenido, date.fromisoformat(fecha_iso))
            log.info(
                "Archivo %s: %s filas validas, %s rechazadas (codificacion=%s, separador=%r)",
                ruta_remota, len(resultado.validas), len(resultado.rechazadas),
                resultado.codificacion, resultado.separador,
            )

            s3, bucket = _s3_hook(), _bucket()
            clave_raw = config.clave_minio("raw", "atenciones", fecha_iso, Path(ruta_remota).name)
            clave_staged = config.clave_minio("staged", "atenciones", fecha_iso, "atenciones_validas.csv")
            s3.load_bytes(contenido, clave_raw, bucket, replace=True)  # copia fiel del original
            s3.load_bytes(
                filas_a_csv(resultado.validas, [*COLUMNAS_CANONICAS, "_numero_fila"]),
                clave_staged, bucket, replace=True,
            )
            if resultado.rechazadas:
                s3.load_bytes(
                    filas_a_csv(
                        resultado.rechazadas,
                        [*COLUMNAS_CANONICAS, "_numero_fila", "motivo_rechazo"],
                    ),
                    config.clave_minio("cuarentena", "atenciones", fecha_iso, "atenciones_rechazadas.csv"),
                    bucket, replace=True,
                )

            umbral = float(Variable.get(config.VAR_UMBRAL_RECHAZO, default=config.UMBRAL_RECHAZO_POR_DEFECTO))
            verificar_umbral_rechazo(resultado, umbral)  # falla explicita si se rechaza demasiado
            return {
                "fecha": fecha_iso,
                "clave_staged": clave_staged,
                "filas_validas": len(resultado.validas),
                "filas_rechazadas": len(resultado.rechazadas),
            }

        transferido = transferir_atenciones_a_minio(fecha_iso)
        fecha_iso >> esperar_archivo >> transferido  # el sensor usa la fecha via XCom
        return transferido

    # ------------------------------------------------------------------
    # 1b) Fuente API: calidad del aire (OpenAQ v3)
    # ------------------------------------------------------------------
    @task_group(group_id="ingesta_calidad_aire_api")
    def ingesta_calidad_aire(fecha_iso: str):

        @task(retries=3)
        def listar_sensores_pm25(fecha_iso: str) -> list[dict]:
            from airflow.sdk import Connection, Variable

            from salud_aire.calidad_aire import ClienteOpenAQ, seleccionar_sensores_pm25
            from salud_aire.simulacion import sensores_simulados

            fuente = Variable.get(config.VAR_FUENTE_CALIDAD_AIRE, default="openaq")
            if fuente == "simulada":
                log.warning("FUENTE SIMULADA activa (Variable %s).", config.VAR_FUENTE_CALIDAD_AIRE)
                return sensores_simulados()
            if fuente != "openaq":
                raise ValueError(f"Variable {config.VAR_FUENTE_CALIDAD_AIRE}='{fuente}' invalida")

            cliente = ClienteOpenAQ(api_key=Connection.get(config.CONN_OPENAQ).password)
            ubicaciones = cliente.listar_ubicaciones(
                *config.CENTRO_LIMA, config.RADIO_BUSQUEDA_M, config.PARAMETRO_PM25_ID
            )
            sensores = seleccionar_sensores_pm25(
                ubicaciones, date.fromisoformat(fecha_iso), config.MAX_SENSORES
            )
            log.info("%s ubicaciones encontradas; %s sensores PM2.5 activos", len(ubicaciones), len(sensores))
            if not sensores:
                raise RuntimeError(
                    "OpenAQ no tiene sensores PM2.5 activos cerca de Lima para "
                    f"{fecha_iso}. Revisar la API o usar FUENTE_CALIDAD_AIRE=simulada."
                )
            return sensores

        @task(
            pool=config.POOL_OPENAQ,  # maximo 2 llamadas simultaneas a la API
            retries=4,
            retry_delay=timedelta(minutes=1),
            retry_exponential_backoff=True,
            max_active_tis_per_dagrun=4,
        )
        def extraer_promedio_diario(sensor: dict, fecha_iso: str) -> dict | None:
            from airflow.sdk import Connection, Variable

            from salud_aire.calidad_aire import ClienteOpenAQ, parsear_promedio_diario
            from salud_aire.simulacion import medicion_simulada

            dia = date.fromisoformat(fecha_iso)
            if Variable.get(config.VAR_FUENTE_CALIDAD_AIRE, default="openaq") == "simulada":
                return medicion_simulada(sensor, dia)
            cliente = ClienteOpenAQ(api_key=Connection.get(config.CONN_OPENAQ).password)
            fila = parsear_promedio_diario(
                cliente.promedios_diarios_sensor(sensor["sensor_id"], dia), sensor, dia
            )
            if fila is None:
                log.warning("Sensor %s sin promedio diario para %s", sensor["sensor_id"], fecha_iso)
            return fila

        @task
        def consolidar_mediciones(mediciones: list, fecha_iso: str) -> dict:
            filas = [m for m in mediciones if m]
            if not filas:
                raise RuntimeError(f"Ninguna estacion reporto PM2.5 para {fecha_iso}")
            clave = config.clave_minio("raw", "calidad_aire", fecha_iso, "mediciones_pm25.json")
            cuerpo = "\n".join(json.dumps(f, ensure_ascii=False) for f in filas).encode("utf-8")
            _s3_hook().load_bytes(cuerpo, clave, _bucket(), replace=True)
            log.info("%s mediciones guardadas en %s", len(filas), clave)
            return {"fecha": fecha_iso, "clave_raw": clave, "estaciones": len(filas)}

        sensores = listar_sensores_pm25(fecha_iso)
        mediciones = extraer_promedio_diario.partial(fecha_iso=fecha_iso).expand(sensor=sensores)
        return consolidar_mediciones(mediciones, fecha_iso)

    # ------------------------------------------------------------------
    # 3) Carga a Snowflake (RAW)
    # ------------------------------------------------------------------
    @task_group(group_id="carga_snowflake_raw")
    def carga_snowflake(info_atenciones: dict, info_aire: dict):

        @task(retries=3, retry_delay=timedelta(minutes=1))
        def cargar_atenciones(info: dict) -> int:
            from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

            from salud_aire.atenciones import csv_a_filas
            from salud_aire.carga_snowflake import reemplazar_particion

            contenido = _s3_hook().read_key(info["clave_staged"], _bucket()).encode("utf-8")
            filas = csv_a_filas(contenido)
            origen = f"s3://{_bucket()}/{info['clave_staged']}"
            for fila in filas:
                fila["_archivo_origen"] = origen
            n = reemplazar_particion(
                SnowflakeHook(snowflake_conn_id=config.CONN_SNOWFLAKE).get_conn(),
                config.TABLA_RAW_ATENCIONES, "FECHA_ATENCION", info["fecha"],
                COLUMNAS_RAW_ATENCIONES, filas,
            )
            log.info("%s filas cargadas en %s para %s", n, config.TABLA_RAW_ATENCIONES, info["fecha"])
            return n

        @task(retries=3, retry_delay=timedelta(minutes=1))
        def cargar_mediciones(info: dict) -> int:
            from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

            from salud_aire.carga_snowflake import reemplazar_particion

            texto = _s3_hook().read_key(info["clave_raw"], _bucket())
            filas = [json.loads(linea) for linea in texto.splitlines() if linea.strip()]
            origen = f"s3://{_bucket()}/{info['clave_raw']}"
            for fila in filas:
                fila["_archivo_origen"] = origen
            n = reemplazar_particion(
                SnowflakeHook(snowflake_conn_id=config.CONN_SNOWFLAKE).get_conn(),
                config.TABLA_RAW_MEDICIONES, "FECHA", info["fecha"],
                COLUMNAS_RAW_MEDICIONES, filas,
            )
            log.info("%s filas cargadas en %s para %s", n, config.TABLA_RAW_MEDICIONES, info["fecha"])
            return n

        return [cargar_atenciones(info_atenciones), cargar_mediciones(info_aire)]

    # ------------------------------------------------------------------
    # 4) Transformacion con dbt (Cosmos): cada modelo/test es una tarea
    # ------------------------------------------------------------------
    transformacion_dbt = DbtTaskGroup(
        group_id="transformacion_dbt",
        project_config=ProjectConfig(DBT_PROJECT_DIR),
        profile_config=ProfileConfig(
            profile_name="salud_aire",
            target_name="dev",
            profiles_yml_filepath=DBT_PROJECT_DIR / "profiles.yml",
        ),
        execution_config=ExecutionConfig(
            dbt_executable_path=DBT_EXECUTABLE_PATH,
            invocation_mode=InvocationMode.SUBPROCESS,  # dbt vive en su propio venv
        ),
        render_config=RenderConfig(
            load_method=LoadMode.DBT_LS,
            dbt_executable_path=DBT_EXECUTABLE_PATH,
            invocation_mode=InvocationMode.SUBPROCESS,
            test_behavior=TestBehavior.AFTER_EACH,
            # Tests con varios padres (relationships, conciliacion) en una tarea
            # propia que corre cuando TODOS sus modelos/seeds ya existen.
            should_detach_multiple_parents_tests=True,
            dbt_deps=False,
        ),
        operator_args={"install_deps": False},
        default_args={
            "owner": "analytics_salud",
            "retries": 1,
            "retry_delay": timedelta(minutes=2),
        },
    )

    # ------------------------------------------------------------------
    # 5) Consumo: reporte diario a partir del mart
    # ------------------------------------------------------------------
    @task(owner="analytics_salud")
    def publicar_reporte(fecha_iso: str) -> str:
        from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

        from salud_aire.atenciones import filas_a_csv
        from salud_aire.reporte import COLUMNAS_REPORTE, construir_reporte

        hook = SnowflakeHook(snowflake_conn_id=config.CONN_SNOWFLAKE)
        sql = f"SELECT {', '.join(COLUMNAS_REPORTE)} FROM {config.TABLA_MART} WHERE fecha = %(fecha)s"
        filas = [
            dict(zip(COLUMNAS_REPORTE, registro, strict=True))
            for registro in hook.get_records(sql, parameters={"fecha": fecha_iso})
        ]
        filas = [
            {k: (float(v) if k in ("pm25_promedio_24h", "variacion_vs_7d_pct") and v is not None else v)
             for k, v in f.items()}
            for f in filas
        ]
        reporte = construir_reporte(fecha_iso, filas)
        s3, bucket = _s3_hook(), _bucket()
        clave_md = config.clave_minio("reportes", "salud_aire", fecha_iso, "reporte.md")
        s3.load_bytes(reporte.encode("utf-8"), clave_md, bucket, replace=True)
        s3.load_bytes(
            filas_a_csv(filas, COLUMNAS_REPORTE),
            config.clave_minio("reportes", "salud_aire", fecha_iso, "reporte.csv"),
            bucket, replace=True,
        )
        log.info("Reporte publicado en s3://%s/%s\n%s", bucket, clave_md, reporte)
        return clave_md

    atenciones = ingesta_atenciones(fecha)
    aire = ingesta_calidad_aire(fecha)
    cargas = carga_snowflake(atenciones, aire)
    reporte = publicar_reporte(fecha)

    cargas >> transformacion_dbt >> reporte


pipeline_salud_aire()
