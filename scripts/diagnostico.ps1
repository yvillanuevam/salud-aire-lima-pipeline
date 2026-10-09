# scripts/diagnostico.ps1
# Junta en logs_ejecucion/diagnostico.log todo lo necesario para revisar un problema:
# estado de los contenedores, errores de import de DAGs, logs recientes,
# tests dentro del contenedor y dbt debug. No muestra contrasenas.

$ErrorActionPreference = "Continue"
$raiz = Split-Path -Parent $PSScriptRoot
Set-Location $raiz
$dirLogs = Join-Path $raiz "logs_ejecucion"
New-Item -ItemType Directory -Force -Path $dirLogs | Out-Null
$marca = Join-Path $dirLogs "diagnostico.estado"
Set-Content -Path $marca -Value "EN_CURSO $(Get-Date -Format s)"
Start-Transcript -Path (Join-Path $dirLogs "diagnostico.log") -Force | Out-Null

function Ejecutar([string]$comando) {
    Write-Host ""
    Write-Host ">>> $comando" -ForegroundColor Cyan
    cmd /c "$comando 2>&1" | Out-Host
}

Ejecutar "docker compose ps -a"
Ejecutar "docker compose exec -T airflow-scheduler airflow dags list-import-errors"
Ejecutar "docker compose exec -T airflow-scheduler airflow dags list"
Ejecutar "docker compose exec -T airflow-scheduler airflow pools list"
Ejecutar "docker compose exec -T airflow-scheduler bash -c `"airflow connections list -o table 2>/dev/null | cut -c1-60`""
Ejecutar "docker compose exec -T airflow-scheduler pytest /opt/airflow/tests -q -p no:cacheprovider"
Ejecutar "docker compose exec -T airflow-scheduler bash -c `"cd /opt/airflow/dags/dbt/salud_aire && /opt/airflow/dbt_venv/bin/dbt debug --profiles-dir . --target-path /tmp/dbt_target --log-path /tmp/dbt_logs`""
foreach ($s in @("sftp-keygen", "minio-init", "airflow-init", "airflow-scheduler", "airflow-dag-processor", "airflow-apiserver")) {
    Ejecutar "docker compose logs --tail 60 $s"
}

Stop-Transcript | Out-Null
Set-Content -Path $marca -Value "OK $(Get-Date -Format s)"
