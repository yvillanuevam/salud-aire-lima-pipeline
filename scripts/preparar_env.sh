#!/usr/bin/env bash
# scripts/preparar_env.sh — equivalente Linux/Mac de preparar_env.ps1.
# Crea .env desde .env.example y genera secretos aleatorios. No sobreescribe.
set -euo pipefail

RAIZ="$(cd "$(dirname "$0")/.." && pwd)"
DESTINO="$RAIZ/.env"

if [ -f "$DESTINO" ]; then
  echo ".env ya existe; no se modifica. Borralo si quieres regenerarlo."
  exit 0
fi

secreto() { openssl rand -base64 "$1" | tr '+/' '-_' | tr -d '=\n'; }
fernet() { openssl rand -base64 32 | tr '+/' '-_' | tr -d '\n'; }

ADMIN_PASS="$(secreto 12)"

sed \
  -e "s|^AIRFLOW_UID=.*|AIRFLOW_UID=$(id -u)|" \
  -e "s|^AIRFLOW_FERNET_KEY=$|AIRFLOW_FERNET_KEY=$(fernet)|" \
  -e "s|^AIRFLOW_JWT_SECRET=$|AIRFLOW_JWT_SECRET=$(secreto 32)|" \
  -e "s|^AIRFLOW_DB_PASSWORD=$|AIRFLOW_DB_PASSWORD=$(secreto 18)|" \
  -e "s|^AIRFLOW_ADMIN_PASSWORD=$|AIRFLOW_ADMIN_PASSWORD=${ADMIN_PASS}|" \
  -e "s|^MINIO_ROOT_PASSWORD=$|MINIO_ROOT_PASSWORD=$(secreto 18)|" \
  -e "s|^MINIO_AIRFLOW_SECRET_KEY=$|MINIO_AIRFLOW_SECRET_KEY=$(secreto 24)|" \
  "$RAIZ/.env.example" > "$DESTINO"

chmod 600 "$DESTINO"
echo ".env creado con secretos aleatorios."
echo "Usuario admin de Airflow: admin  /  contrasena: ${ADMIN_PASS}"
echo "Ahora completa SNOWFLAKE_* y OPENAQ_API_KEY en .env"
