-- =====================================================================
-- 02_operaciones_utiles.sql — operaciones de Snowflake vistas en Clase 7
-- aplicadas al proyecto. NO es necesario correrlas para que el pipeline
-- funcione; son recetas para operacion y para la demo.
-- =====================================================================

USE ROLE SYSADMIN;
USE WAREHOUSE WH_SALUD_AIRE;

-- 1) Zero-Copy Cloning: un ambiente de desarrollo identico a produccion en
--    segundos y sin duplicar almacenamiento (solo se paga lo que cambie).
CREATE DATABASE IF NOT EXISTS SALUD_AIRE_DB_DEV CLONE SALUD_AIRE_DB;
-- ...y para borrarlo cuando ya no se use:
-- DROP DATABASE SALUD_AIRE_DB_DEV;

-- 2) Time Travel: ver la tabla RAW tal como estaba hace 30 minutos
--    (por ejemplo, antes de una recarga equivocada).
SELECT COUNT(*) AS filas_hace_30_min
FROM SALUD_AIRE_DB.RAW.ATENCIONES_RESPIRATORIAS AT (OFFSET => -60 * 30);

-- Restaurar una tabla borrada por error (dentro del periodo de retencion):
-- UNDROP TABLE SALUD_AIRE_DB.RAW.ATENCIONES_RESPIRATORIAS;

-- 3) Costos: el warehouse solo consume creditos mientras esta encendido.
SHOW WAREHOUSES LIKE 'WH_SALUD_AIRE';   -- revisar AUTO_SUSPEND = 60
-- Consumo de creditos de los ultimos 7 dias (requiere ACCOUNTADMIN):
-- SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
-- WHERE WAREHOUSE_NAME = 'WH_SALUD_AIRE' AND START_TIME > DATEADD(day, -7, CURRENT_TIMESTAMP());

-- 4) Auditoria: filas que los tests de dbt marcaron (store_failures).
SHOW TABLES IN SCHEMA SALUD_AIRE_DB.AUDITORIA;
