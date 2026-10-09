# Guía de entrega — pasos que el equipo hace a mano

El código está completo, pero hay cosas que **solo el equipo puede hacer** (y que el profesor
revisa): el historial de Git, el repositorio público, la Branch Protection, el Pull Request con
el CI en verde y el video. Esta guía las ordena.

---

## 1. Historial de commits real (no un commit gigante)

El enunciado penaliza "un solo commit *proyecto final completo*". La forma honesta de lograr un
historial legible es **subir el proyecto por partes, a medida que cada parte se prueba en su
máquina**. Orden sugerido (cada línea = un commit; las marcadas con **PR** van en una rama + Pull Request):

| # | Qué se agrega | Se prueba con | Mensaje sugerido |
|---|---|---|---|
| 1 | `.gitignore`, `.gitattributes`, `.env.example`, `scripts/`, `README.md` (borrador) | `scripts/preparar_env` crea `.env` | `chore: estructura inicial y plantilla de configuracion` |
| 2 | `Dockerfile`, `docker-compose.yaml`, `requirements*.txt`, `infra/minio/` | `docker compose up` → todo *healthy* | `feat(infra): Airflow 3 + SFTP + MinIO con docker compose` |
| 3 | `dags/salud_aire/{config,fechas,simulacion}.py`, `dags/simulador_envio_clinicas.py` | El simulador deja el CSV en el SFTP | `feat: simulador del proveedor SFTP de clinicas` |
| 4 | `dags/salud_aire/atenciones.py` + `tests/test_atenciones.py`, `pytest.ini`, `pyproject.toml` | `pytest` | `feat: validacion del CSV de atenciones con cuarentena` |
| 5 | `.github/workflows/ci.yml` — **PR** | El check `lint-and-test` corre en el PR | `ci: workflow de lint y tests en pull requests` |
| 6 | Branch Protection (sección 3) | Ya no se puede pushear directo a `main` | *(configuración, sin commit)* |
| 7 | `calidad_aire.py` + `tests/test_calidad_aire.py` + fixtures — **PR** | CI en verde | `feat: cliente OpenAQ con reintentos y backoff` |
| 8 | `infra/snowflake/`, `carga_snowflake.py`, tests — **PR** | Script SQL corrido en Snowsight | `feat: carga idempotente a Snowflake RAW` |
| 9 | `dags/dbt/salud_aire/` — **PR** | `dbt debug` y `dbt build` dentro del contenedor | `feat(dbt): modelos staging, intermediate y marts con tests` |
| 10 | `dags/pipeline_salud_aire.py`, `reporte.py`, `alertas.py`, `tests/test_dags.py`, `tests/test_seguridad.py` — **PR** | El DAG corre de punta a punta | `feat: DAG productivo con Cosmos y reporte diario` |
| 11 | `.github/workflows/cd.yml` — **PR** | Al mergear se publica la imagen en GHCR | `cd: publicar imagen de Airflow al mergear a main` |
| 12 | `docs/`, README final — **PR** | — | `docs: arquitectura, glosario y guia de operacion` |

> Lo ideal es que **cada integrante haga sus propios commits** (con su usuario de GitHub) y que
> estos se repartan en varios días, como en el cronograma del enunciado.

Antes del primer `git add`, confirmar que `.env` **no** aparece en `git status`.

## 2. Crear el repositorio público y subir

```powershell
git init
git branch -M main
git add .gitignore .gitattributes .env.example scripts README.md
git commit -m "chore: estructura inicial y plantilla de configuracion"
git remote add origin https://github.com/<usuario>/salud-aire-lima-pipeline.git
git push -u origin main
```

En GitHub: *New repository* → nombre `salud-aire-lima-pipeline` → **Public** (obligatorio) → sin README.

Para cada PR:

```powershell
git checkout -b feature/cliente-openaq
git add dags/salud_aire/calidad_aire.py tests/test_calidad_aire.py tests/fixtures
git commit -m "feat: cliente OpenAQ con reintentos y backoff"
git push -u origin feature/cliente-openaq
# En GitHub: Compare & pull request -> esperar check verde -> Merge -> borrar la rama
git checkout main; git pull
```

## 3. Branch Protection y Environment

1. **Settings → Branches → Add branch protection rule** (o *Rulesets*).
   - *Branch name pattern*: `main`
   - ✅ *Require a pull request before merging*
   - ✅ *Require status checks to pass before merging* → buscar y elegir **`lint-and-test`**
     (aparece después de que el CI corrió al menos una vez en un PR).
   - ✅ *Do not allow bypassing the above settings* (opcional, recomendado).
2. **Settings → Environments → New environment** → `produccion` (lo usa `cd.yml`).
   Opcional: *Required reviewers* para aprobar cada despliegue.
3. **Settings → Actions → General → Workflow permissions**: dejar *Read repository contents*;
   `cd.yml` pide `packages: write` solo para su job.

Captura de pantalla de la regla para el documento de arquitectura o el video.

## 4. Checklist final (sección 13 del enunciado)

```powershell
docker compose down -v; docker compose build; docker compose up airflow-init; docker compose up -d
git log --oneline            # historial real, varios autores/fechas
git log --all -- .env        # NO debe mostrar nada
docker compose exec airflow-scheduler bash -c "cd /opt/airflow/dags/dbt/salud_aire && /opt/airflow/dbt_venv/bin/dbt build --profiles-dir ."
```

- [ ] `docker compose up` levanta todo desde cero en otra máquina siguiendo solo el README.
- [ ] Último PR con el check `lint-and-test` en verde antes del merge.
- [ ] `dbt build` pasa (los `warn` de distritos sin mapear son esperados).
- [ ] Ningún archivo con credenciales en el historial.
- [ ] Video grabado con el pipeline corriendo de verdad.
- [ ] PDF de arquitectura con el link del video y del repositorio.

## 5. Guion del video (5-8 min)

| Min | Qué mostrar |
|---|---|
| 0:00-0:45 | El problema en 2 frases y el diagrama (`docs/arquitectura.png`). |
| 0:45-1:45 | `docker compose ps` todo *healthy*; `.env.example` vs. `.gitignore` (dónde viven los secretos); UI de Airflow con los 2 DAGs. |
| 1:45-3:30 | Disparar el simulador y luego `pipeline_salud_aire`. Mostrar en *Graph*: sensor en `reschedule`, *mapped tasks* de la API con el pool, TaskGroups y las tareas de Cosmos (una por modelo y test). |
| 3:30-4:30 | MinIO: `raw/`, `staged/`, `cuarentena/` (abrir el CSV de rechazados con su motivo) y `reportes/`. |
| 4:30-5:30 | Snowflake: `RAW` → `MARTS.FCT_SALUD_AIRE_DISTRITO_DIA`; una consulta de `analyses/consultas_consumo.sql`; `AUDITORIA.DISTRITOS_SIN_MAPEAR` (CALLAO). |
| 5:30-7:00 | GitHub: un PR con el check `lint-and-test` en verde, la Branch Protection, la pestaña Actions con el CD publicando la imagen, y `git log`. |
| 7:00-7:30 | Cierre: qué se haría para producción (Secrets Backend, Celery/Kubernetes, alertas a Slack). |

Subir a YouTube (no listado) o Google Drive y pegar el link en el PDF de arquitectura.
