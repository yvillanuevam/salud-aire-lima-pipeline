"""Constantes compartidas por los DAGs. Aqui NO hay credenciales: solo
identificadores de Connections/Variables de Airflow y nombres de objetos."""

from __future__ import annotations

ZONA_HORARIA = "America/Lima"

# --- Connections de Airflow (se definen en docker-compose via AIRFLOW_CONN_*) ---
CONN_SFTP = "sftp_clinicas"
CONN_MINIO = "minio_datalake"
CONN_OPENAQ = "openaq_api"
CONN_SNOWFLAKE = "snowflake_salud_aire"

# --- Variables de Airflow (con valores por defecto si no existen) ---
VAR_FUENTE_CALIDAD_AIRE = "fuente_calidad_aire"  # "openaq" | "simulada"
VAR_BUCKET = "bucket_datalake"
VAR_UMBRAL_RECHAZO = "umbral_rechazo_atenciones"
BUCKET_POR_DEFECTO = "salud-aire-datalake"
UMBRAL_RECHAZO_POR_DEFECTO = 0.20

# --- Pools ---
POOL_OPENAQ = "api_openaq"

# --- SFTP ---
CARPETA_SFTP_ATENCIONES = "/upload/atenciones"


def ruta_sftp_atenciones(fecha_iso: str) -> str:
    """Ruta del archivo diario que deja la red de clinicas en el SFTP."""
    return f"{CARPETA_SFTP_ATENCIONES}/atenciones_{fecha_iso}.csv"


# --- Data lake (MinIO / S3): particionado por fecha estilo Hive ---
def clave_minio(capa: str, dominio: str, fecha_iso: str, archivo: str) -> str:
    """Construye la clave de un objeto en el data lake.

    >>> clave_minio("raw", "atenciones", "2026-10-07", "a.csv")
    'raw/atenciones/fecha=2026-10-07/a.csv'
    """
    capas_validas = {"raw", "staged", "cuarentena", "reportes"}
    if capa not in capas_validas:
        raise ValueError(f"Capa '{capa}' invalida; usa una de {sorted(capas_validas)}")
    return f"{capa}/{dominio}/fecha={fecha_iso}/{archivo}"


# --- Snowflake ---
TABLA_RAW_ATENCIONES = "SALUD_AIRE_DB.RAW.ATENCIONES_RESPIRATORIAS"
TABLA_RAW_MEDICIONES = "SALUD_AIRE_DB.RAW.MEDICIONES_AIRE"
TABLA_MART = "SALUD_AIRE_DB.MARTS.FCT_SALUD_AIRE_DISTRITO_DIA"

# --- OpenAQ ---
CENTRO_LIMA = (-12.0464, -77.0428)  # Plaza Mayor de Lima
RADIO_BUSQUEDA_M = 25_000  # maximo permitido por /v3/locations
PARAMETRO_PM25_ID = 2  # id del parametro pm25 en OpenAQ
MAX_SENSORES = 20  # tope de llamadas por corrida (cuida el limite de la API)
