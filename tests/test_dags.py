"""Tests estructurales de los DAGs y de buenas practicas de seguridad.

Cargan los DAGs igual que lo haria el dag-processor de Airflow (DagBag). El
DAG principal renderiza el proyecto dbt con Cosmos (dbt ls), por eso en CI se
instala dbt en un venv aparte y se definen credenciales FICTICIAS de Snowflake
(dbt ls no se conecta a Snowflake).
"""

from pathlib import Path

import pytest

RAIZ = Path(__file__).resolve().parents[1]
DAG_PRINCIPAL = "pipeline_salud_aire"
DAGS_ESPERADOS = {DAG_PRINCIPAL, "simulador_envio_clinicas"}

pytestmark = pytest.mark.dagbag


@pytest.fixture(scope="session")
def dagbag():
    from airflow.models import DagBag

    return DagBag(dag_folder=str(RAIZ / "dags"), include_examples=False)


def test_no_hay_errores_de_import(dagbag):
    assert dagbag.import_errors == {}, f"DAGs rotos: {dagbag.import_errors}"


def test_estan_exactamente_los_dags_esperados(dagbag):
    assert set(dagbag.dag_ids) == DAGS_ESPERADOS


def test_dags_programados_sin_catchup_y_documentados(dagbag):
    for dag in dagbag.dags.values():
        assert dag.schedule is not None, f"{dag.dag_id} no tiene schedule"
        assert dag.catchup is False, f"{dag.dag_id} tiene catchup activado"
        assert dag.doc_md and len(dag.doc_md) > 200, f"{dag.dag_id} sin doc_md"
        assert dag.tags, f"{dag.dag_id} sin tags"


def test_todas_las_tareas_tienen_owner_real_y_reintentos(dagbag):
    for dag in dagbag.dags.values():
        for tarea in dag.tasks:
            assert tarea.owner not in (None, "", "airflow"), f"{tarea.task_id} sin owner"
            assert tarea.retries >= 1, f"{dag.dag_id}.{tarea.task_id} sin retries"


def test_pipeline_tiene_las_etapas_de_la_arquitectura(dagbag):
    dag = dagbag.get_dag(DAG_PRINCIPAL)
    grupos = set(dag.task_group_dict)
    assert {
        "ingesta_atenciones_sftp",
        "ingesta_calidad_aire_api",
        "carga_snowflake_raw",
        "transformacion_dbt",
    } <= grupos
    assert "publicar_reporte" in dag.task_ids


def test_sensor_sftp_en_modo_reschedule_con_timeout(dagbag):
    sensor = dagbag.get_dag(DAG_PRINCIPAL).get_task("ingesta_atenciones_sftp.esperar_archivo_atenciones")
    assert sensor.mode == "reschedule"
    assert 0 < sensor.timeout <= 6 * 3600


def test_llamadas_a_la_api_limitadas_por_pool(dagbag):
    tarea = dagbag.get_dag(DAG_PRINCIPAL).get_task("ingesta_calidad_aire_api.extraer_promedio_diario")
    assert tarea.pool == "api_openaq"
    assert tarea.retry_exponential_backoff


def test_cosmos_genera_una_tarea_por_modelo_dbt(dagbag):
    ids = dagbag.get_dag(DAG_PRINCIPAL).task_ids
    for modelo in ("stg_atenciones", "stg_mediciones_aire", "int_atenciones_distrito_dia",
                   "int_pm25_distrito_dia", "fct_salud_aire_distrito_dia"):
        assert any(modelo in t for t in ids), f"No hay tarea de Cosmos para {modelo}"


def test_orden_carga_dbt_reporte(dagbag):
    dag = dagbag.get_dag(DAG_PRINCIPAL)
    reporte = dag.get_task("publicar_reporte")
    ancestros = {t.task_id for t in reporte.get_flat_relatives(upstream=True)}
    assert "carga_snowflake_raw.cargar_atenciones" in ancestros
    assert "carga_snowflake_raw.cargar_mediciones" in ancestros
    assert any(t.startswith("transformacion_dbt.") for t in ancestros)
