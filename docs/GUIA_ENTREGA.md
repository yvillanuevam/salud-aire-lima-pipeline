# Guía de entrega

Resumen de lo que el equipo hace a mano para cerrar los tres entregables
(repositorio público, video demo y documento de arquitectura). La versión
detallada, paso a paso y para principiantes, es el documento Word
*Guía paso a paso — Salud-Aire Lima* que acompaña al proyecto.

## 1. Branch Protection (una vez)

*Settings → Branches → Add classic branch protection rule* → patrón `main` →
✅ *Require a pull request before merging* → ✅ *Require status checks to pass*
→ elegir **`lint-and-test`** → *Create*.

## 2. Levantar y correr el pipeline

1. Completar `SNOWFLAKE_*` y `OPENAQ_API_KEY` en `.env` y correr
   `infra/snowflake/01_setup_snowflake.sql` en Snowsight.
2. Detener otros proyectos de Docker que no se usen (Airflow consume mucha RAM).
3. Doble clic en `scripts\levantar.bat` → debe terminar en **LISTO**.
4. Doble clic en `scripts\diagnostico.bat` → Connections en **OK** y
   `dbt debug` con **All checks passed!**
5. Doble clic en `scripts\ejecutar_8_dias.bat` → procesa los 8 días previos.

## 3. Cambios del equipo (historial real)

Cada cambio nuevo va en una rama y un Pull Request; el check `lint-and-test`
debe quedar en verde antes del merge. Ejemplos: nombres del equipo y link del
video en el README, alias nuevos de distritos en
`dags/dbt/salud_aire/seeds/alias_distritos.csv`, capturas en `docs/`.

## 4. Checklist final

- [ ] `levantar.bat` levanta todo desde cero siguiendo solo el README.
- [ ] `git log --all -- .env` no muestra nada.
- [ ] El último Pull Request tiene `lint-and-test` en verde antes del merge.
- [ ] Branch Protection activa en `main`.
- [ ] `MARTS.FCT_SALUD_AIRE_DISTRITO_DIA` con datos de 8 días.
- [ ] Video grabado con el pipeline corriendo de verdad.
- [ ] PDF de arquitectura con el link del video y del repositorio.

## 5. Guion del video (5-8 min)

| Min | Qué mostrar |
|---|---|
| 0:00-0:45 | El problema en 2 frases y el diagrama (`docs/arquitectura.png`). |
| 0:45-1:45 | Docker Desktop con los contenedores en verde; `.env.example` y `.gitignore` (dónde viven los secretos); Airflow con los 2 DAGs. |
| 1:45-3:30 | Disparar el simulador y luego `pipeline_salud_aire`. En *Graph*: sensor en `reschedule`, tareas mapeadas de la API con el pool, TaskGroups y las tareas de Cosmos. |
| 3:30-4:30 | MinIO: `raw/`, `staged/`, `cuarentena/` (CSV de rechazados con su motivo) y `reportes/`. |
| 4:30-5:30 | Snowflake: el mart final, una consulta de `analyses/consultas_consumo.sql` y `AUDITORIA.DISTRITOS_SIN_MAPEAR`. |
| 5:30-7:00 | GitHub: un Pull Request con `lint-and-test` en verde y su merge, la Branch Protection, Actions con el CD y el historial de commits. |
| 7:00-7:30 | Cierre: qué se haría en producción (Secrets Backend, más workers, alertas). |
