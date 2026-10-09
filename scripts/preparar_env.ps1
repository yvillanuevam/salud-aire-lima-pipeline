# scripts/preparar_env.ps1
# Crea el archivo .env a partir de .env.example y genera secretos aleatorios
# para los servicios internos (Fernet key, JWT, Postgres, MinIO, admin de Airflow).
# Si .env ya existe, NO lo sobreescribe.
#
# Uso (desde la raiz del repositorio):
#   powershell -ExecutionPolicy Bypass -File .\scripts\preparar_env.ps1

$ErrorActionPreference = "Stop"
$raiz = Split-Path -Parent $PSScriptRoot
$plantilla = Join-Path $raiz ".env.example"
$destino = Join-Path $raiz ".env"

if (Test-Path $destino) {
    Write-Host ".env ya existe; no se modifica. Borralo si quieres regenerarlo." -ForegroundColor Yellow
    exit 0
}

function Nuevo-Secreto([int]$bytes = 24) {
    $buffer = New-Object byte[] $bytes
    [System.Security.Cryptography.RandomNumberGenerator]::Create().GetBytes($buffer)
    # Base64 "url-safe" sin relleno: solo letras, numeros, - y _
    return [Convert]::ToBase64String($buffer).Replace('+', '-').Replace('/', '_').TrimEnd('=')
}

function Nueva-FernetKey {
    # Una Fernet key es base64 url-safe de exactamente 32 bytes (CON relleno '=').
    $buffer = New-Object byte[] 32
    [System.Security.Cryptography.RandomNumberGenerator]::Create().GetBytes($buffer)
    return [Convert]::ToBase64String($buffer).Replace('+', '-').Replace('/', '_')
}

$valores = @{
    "AIRFLOW_FERNET_KEY"       = Nueva-FernetKey
    "AIRFLOW_JWT_SECRET"       = Nuevo-Secreto 32
    "AIRFLOW_DB_PASSWORD"      = Nuevo-Secreto 18
    "AIRFLOW_ADMIN_PASSWORD"   = Nuevo-Secreto 12
    "MINIO_ROOT_PASSWORD"      = Nuevo-Secreto 18
    "MINIO_AIRFLOW_SECRET_KEY" = Nuevo-Secreto 24
}

$lineas = Get-Content $plantilla | ForEach-Object {
    $linea = $_
    foreach ($clave in $valores.Keys) {
        if ($linea -match "^$clave=$") { $linea = "$clave=$($valores[$clave])" }
    }
    $linea
}
# UTF-8 sin BOM: docker compose no acepta BOM al inicio del .env
[System.IO.File]::WriteAllLines($destino, $lineas, (New-Object System.Text.UTF8Encoding $false))

Write-Host ".env creado con secretos aleatorios." -ForegroundColor Green
Write-Host "Usuario admin de Airflow: admin  /  contrasena: $($valores['AIRFLOW_ADMIN_PASSWORD'])"
Write-Host "Ahora completa SNOWFLAKE_* y OPENAQ_API_KEY en .env"
