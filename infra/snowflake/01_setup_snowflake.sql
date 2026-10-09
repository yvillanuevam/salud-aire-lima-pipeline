-- =====================================================================
-- 01_setup_snowflake.sql — Salud-Aire Lima
-- Correr UNA vez en Snowsight con un usuario que tenga ACCOUNTADMIN
-- (la cuenta trial lo tiene). Es idempotente: se puede volver a correr.
--
-- Unico cambio obligatorio: reemplaza TU_USUARIO_SNOWFLAKE (3 lineas al final).
--
-- Modelo de permisos (principio de minimo privilegio):
--   ROLE_SALUD_AIRE_INGESTA        -> Airflow. Escribe SOLO en RAW; lee MARTS
--                                     para generar el reporte final.
--   ROLE_SALUD_AIRE_TRANSFORMACION -> dbt. Lee RAW; crea objetos en STAGING,
--                                     INTERMEDIATE, MARTS y AUDITORIA.
--   ROLE_SALUD_AIRE_LECTOR         -> consumo (BI / analistas). Solo lee MARTS.
-- =====================================================================

-- ---------------------------------------------------------------------
-- 1) Warehouse, base de datos y esquemas (rol SYSADMIN)
-- ---------------------------------------------------------------------
USE ROLE SYSADMIN;

CREATE WAREHOUSE IF NOT EXISTS WH_SALUD_AIRE
  WAREHOUSE_SIZE = 'XSMALL'
  AUTO_SUSPEND = 60            -- se apaga tras 60 s sin uso (ahorro de creditos)
  AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED = TRUE
  COMMENT = 'Warehouse del pipeline Salud-Aire Lima';

CREATE DATABASE IF NOT EXISTS SALUD_AIRE_DB
  DATA_RETENTION_TIME_IN_DAYS = 1  -- Time Travel de 1 dia (maximo del trial Standard)
  COMMENT = 'Calidad del aire y atenciones respiratorias en Lima Metropolitana';

CREATE SCHEMA IF NOT EXISTS SALUD_AIRE_DB.RAW          COMMENT = 'Datos tal como llegan de las fuentes (cargados por Airflow)';
CREATE SCHEMA IF NOT EXISTS SALUD_AIRE_DB.STAGING      COMMENT = 'dbt: limpieza y tipado (vistas) + seeds de referencia';
CREATE SCHEMA IF NOT EXISTS SALUD_AIRE_DB.INTERMEDIATE COMMENT = 'dbt: reglas de negocio intermedias (vistas)';
CREATE SCHEMA IF NOT EXISTS SALUD_AIRE_DB.MARTS        COMMENT = 'dbt: modelos finales para consumo (tablas)';
CREATE SCHEMA IF NOT EXISTS SALUD_AIRE_DB.AUDITORIA    COMMENT = 'dbt: filas que fallan tests con store_failures';

-- ---------------------------------------------------------------------
-- 2) Tablas RAW (las llena Airflow con DELETE + INSERT por fecha: idempotente)
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS SALUD_AIRE_DB.RAW.ATENCIONES_RESPIRATORIAS (
    FECHA_ATENCION     DATE           NOT NULL,
    CODIGO_CLINICA     VARCHAR(20)    NOT NULL,
    DISTRITO           VARCHAR(100)   NOT NULL,  -- tal como lo escribe la clinica
    GRUPO_EDAD         VARCHAR(10)    NOT NULL,
    DIAGNOSTICO_CIE10  VARCHAR(10)    NOT NULL,
    NUM_ATENCIONES     NUMBER(10, 0)  NOT NULL,
    _NUMERO_FILA       NUMBER(10, 0)  NOT NULL,  -- fila en el archivo original (trazabilidad)
    _ARCHIVO_ORIGEN    VARCHAR(300)   NOT NULL,  -- objeto en MinIO del que se cargo
    _CARGADO_EN        TIMESTAMP_NTZ  DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'Atenciones por enfermedades respiratorias (CIE-10 J00-J99) enviadas por las clinicas via SFTP';

CREATE TABLE IF NOT EXISTS SALUD_AIRE_DB.RAW.MEDICIONES_AIRE (
    FECHA              DATE           NOT NULL,
    ESTACION_ID        VARCHAR(50)    NOT NULL,
    ESTACION_NOMBRE    VARCHAR(200),
    LATITUD            FLOAT,
    LONGITUD           FLOAT,
    SENSOR_ID          VARCHAR(50)    NOT NULL,
    PARAMETRO          VARCHAR(20)    NOT NULL,
    UNIDAD             VARCHAR(20),
    VALOR_PROMEDIO     FLOAT,
    COBERTURA_PCT      FLOAT,
    N_OBSERVACIONES    NUMBER(10, 0),
    FUENTE             VARCHAR(20)    NOT NULL,  -- 'openaq' | 'simulada'
    _ARCHIVO_ORIGEN    VARCHAR(300)   NOT NULL,
    _CARGADO_EN        TIMESTAMP_NTZ  DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'Promedio diario de contaminantes por estacion (API OpenAQ v3)';

-- ---------------------------------------------------------------------
-- 3) Roles y permisos (rol SECURITYADMIN)
-- ---------------------------------------------------------------------
USE ROLE SECURITYADMIN;

CREATE ROLE IF NOT EXISTS ROLE_SALUD_AIRE_INGESTA        COMMENT = 'Airflow: carga RAW y lee MARTS';
CREATE ROLE IF NOT EXISTS ROLE_SALUD_AIRE_TRANSFORMACION COMMENT = 'dbt: lee RAW y construye STAGING/INTERMEDIATE/MARTS';
CREATE ROLE IF NOT EXISTS ROLE_SALUD_AIRE_LECTOR         COMMENT = 'Consumo: solo lectura de MARTS';

-- Jerarquia recomendada: SYSADMIN hereda los roles custom (puede administrarlos).
GRANT ROLE ROLE_SALUD_AIRE_INGESTA        TO ROLE SYSADMIN;
GRANT ROLE ROLE_SALUD_AIRE_TRANSFORMACION TO ROLE SYSADMIN;
GRANT ROLE ROLE_SALUD_AIRE_LECTOR         TO ROLE SYSADMIN;

-- Warehouse: solo USAGE (ninguno puede redimensionarlo ni borrarlo).
GRANT USAGE ON WAREHOUSE WH_SALUD_AIRE TO ROLE ROLE_SALUD_AIRE_INGESTA;
GRANT USAGE ON WAREHOUSE WH_SALUD_AIRE TO ROLE ROLE_SALUD_AIRE_TRANSFORMACION;
GRANT USAGE ON WAREHOUSE WH_SALUD_AIRE TO ROLE ROLE_SALUD_AIRE_LECTOR;

GRANT USAGE ON DATABASE SALUD_AIRE_DB TO ROLE ROLE_SALUD_AIRE_INGESTA;
GRANT USAGE ON DATABASE SALUD_AIRE_DB TO ROLE ROLE_SALUD_AIRE_TRANSFORMACION;
GRANT USAGE ON DATABASE SALUD_AIRE_DB TO ROLE ROLE_SALUD_AIRE_LECTOR;

-- INGESTA: escribe en las tablas RAW existentes (no puede crear ni borrar tablas).
GRANT USAGE ON SCHEMA SALUD_AIRE_DB.RAW TO ROLE ROLE_SALUD_AIRE_INGESTA;
GRANT SELECT, INSERT, DELETE ON TABLE SALUD_AIRE_DB.RAW.ATENCIONES_RESPIRATORIAS TO ROLE ROLE_SALUD_AIRE_INGESTA;
GRANT SELECT, INSERT, DELETE ON TABLE SALUD_AIRE_DB.RAW.MEDICIONES_AIRE          TO ROLE ROLE_SALUD_AIRE_INGESTA;
GRANT USAGE ON SCHEMA SALUD_AIRE_DB.MARTS TO ROLE ROLE_SALUD_AIRE_INGESTA;
GRANT SELECT ON FUTURE TABLES IN SCHEMA SALUD_AIRE_DB.MARTS TO ROLE ROLE_SALUD_AIRE_INGESTA;
GRANT SELECT ON ALL TABLES    IN SCHEMA SALUD_AIRE_DB.MARTS TO ROLE ROLE_SALUD_AIRE_INGESTA;

-- TRANSFORMACION: solo lectura de RAW...
GRANT USAGE ON SCHEMA SALUD_AIRE_DB.RAW TO ROLE ROLE_SALUD_AIRE_TRANSFORMACION;
GRANT SELECT ON ALL TABLES    IN SCHEMA SALUD_AIRE_DB.RAW TO ROLE ROLE_SALUD_AIRE_TRANSFORMACION;
GRANT SELECT ON FUTURE TABLES IN SCHEMA SALUD_AIRE_DB.RAW TO ROLE ROLE_SALUD_AIRE_TRANSFORMACION;
-- ...y creacion de tablas/vistas solo en sus propias capas.
GRANT USAGE, CREATE TABLE, CREATE VIEW ON SCHEMA SALUD_AIRE_DB.STAGING      TO ROLE ROLE_SALUD_AIRE_TRANSFORMACION;
GRANT USAGE, CREATE TABLE, CREATE VIEW ON SCHEMA SALUD_AIRE_DB.INTERMEDIATE TO ROLE ROLE_SALUD_AIRE_TRANSFORMACION;
GRANT USAGE, CREATE TABLE, CREATE VIEW ON SCHEMA SALUD_AIRE_DB.MARTS        TO ROLE ROLE_SALUD_AIRE_TRANSFORMACION;
GRANT USAGE, CREATE TABLE, CREATE VIEW ON SCHEMA SALUD_AIRE_DB.AUDITORIA    TO ROLE ROLE_SALUD_AIRE_TRANSFORMACION;

-- LECTOR: solo MARTS.
GRANT USAGE ON SCHEMA SALUD_AIRE_DB.MARTS TO ROLE ROLE_SALUD_AIRE_LECTOR;
GRANT SELECT ON ALL TABLES    IN SCHEMA SALUD_AIRE_DB.MARTS TO ROLE ROLE_SALUD_AIRE_LECTOR;
GRANT SELECT ON FUTURE TABLES IN SCHEMA SALUD_AIRE_DB.MARTS TO ROLE ROLE_SALUD_AIRE_LECTOR;

-- ---------------------------------------------------------------------
-- 4) Asignar los roles a tu usuario  <-- REEMPLAZA TU_USUARIO_SNOWFLAKE
--    (en un entorno real, cada rol iria a un usuario de servicio distinto)
-- ---------------------------------------------------------------------
GRANT ROLE ROLE_SALUD_AIRE_INGESTA        TO USER TU_USUARIO_SNOWFLAKE;
GRANT ROLE ROLE_SALUD_AIRE_TRANSFORMACION TO USER TU_USUARIO_SNOWFLAKE;
GRANT ROLE ROLE_SALUD_AIRE_LECTOR         TO USER TU_USUARIO_SNOWFLAKE;
