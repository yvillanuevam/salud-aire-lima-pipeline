# Imagen de Airflow del proyecto Salud-Aire Lima.
#
# Parte de la imagen oficial y agrega solo lo que falta:
#   1) Cosmos (y los providers que usan los DAGs) en el entorno de Airflow,
#      fijando la version de Airflow para que pip nunca la actualice por accidente.
#   2) dbt-snowflake en un entorno virtual APARTE (/opt/airflow/dbt_venv).
#      Es la recomendacion de Cosmos: dbt y Airflow tienen muchas dependencias
#      en comun (protobuf, snowflake-connector, jinja...) y mezclarlas en un solo
#      entorno es la causa mas comun de "Broken DAG" al actualizar algo.
ARG AIRFLOW_VERSION=3.0.6
FROM apache/airflow:${AIRFLOW_VERSION}

ARG AIRFLOW_VERSION=3.0.6

USER airflow

COPY --chown=airflow:root requirements.txt /tmp/requirements.txt
COPY --chown=airflow:root requirements-dbt.txt /tmp/requirements-dbt.txt

# Mismo archivo de constraints oficial que usa el CI: CI e imagen resuelven
# exactamente las mismas versiones de librerias.
RUN PYTHON_VERSION="$(python -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')" \
    && pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /tmp/requirements.txt \
       --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

RUN python -m venv /opt/airflow/dbt_venv \
    && /opt/airflow/dbt_venv/bin/pip install --no-cache-dir -r /tmp/requirements-dbt.txt

ENV DBT_EXECUTABLE_PATH=/opt/airflow/dbt_venv/bin/dbt
