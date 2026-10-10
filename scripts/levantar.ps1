# scripts/levantar.ps1
# Levanta todo el ambiente desde cero y deja el resultado en logs_ejecucion/levantar.log
#   1) crea .env si no existe (scripts/preparar_env.ps1)
#   2) docker compose build
#   3) docker compose up airflow-init  (migra la BD, crea el admin y el pool)
#   4) docker compose up -d y espera a que todos los servicios queden "healthy"
#
# Uso: doble clic en scripts\levantar.bat  (o: powershell -ExecutionPolicy Bypass -File scripts\levantar.ps1)

$ErrorActionPreference = "Continue"
$raiz = Split-Path -Parent $PSScriptRoot
Set-Location $raiz
$dirLogs = Join-Path $raiz "logs_ejecucion"
New-Item -ItemType Directory -Force -Path $dirLogs | Out-Null
$log = Join-Path $dirLogs "levantar.log"
$marca = Join-Path $dirLogs "levantar.estado"
Set-Content -Path $marca -Value "EN_CURSO $(Get-Date -Format s)"
Start-Transcript -Path $log -Force | Out-Null

function Ejecutar([string]$comando) {
    Write-Host ""
    Write-Host ">>> $comando" -ForegroundColor Cyan
    cmd /c "$comando 2>&1" | Out-Host
    return $LASTEXITCODE
}

$estado = "ERROR"
try {
    if ((Ejecutar "docker info --format {{.ServerVersion}}") -ne 0) {
        throw "Docker Desktop no esta corriendo. Abrelo, espera a que la ballena quede quieta y vuelve a intentar."
    }
    if (-not (Test-Path (Join-Path $raiz ".env"))) {
        Write-Host "No existe .env: se crea con secretos aleatorios." -ForegroundColor Yellow
        & (Join-Path $PSScriptRoot "preparar_env.ps1")
    }

    if ((Ejecutar "docker compose config --quiet") -ne 0) { throw "docker-compose.yaml o .env invalidos (ver arriba)." }
    if ((Ejecutar "docker compose build --progress plain") -ne 0) { throw "Fallo docker compose build." }
    # airflow-init se corre en segundo plano y se espera su codigo de salida con
    # "docker wait" (mas robusto que quedarse "attached" a su salida).
    if ((Ejecutar "docker compose up -d --force-recreate airflow-init") -ne 0) { throw "No se pudo iniciar airflow-init." }
    $idInit = (cmd /c "docker compose ps -a -q airflow-init" | Select-Object -First 1)
    $codigo = (cmd /c "docker wait $idInit" | Select-Object -Last 1)
    Ejecutar "docker compose logs --no-color --tail 15 airflow-init" | Out-Null
    if ("$codigo".Trim() -ne "0") { throw "airflow-init termino con codigo $codigo (ver logs arriba)." }
    if ((Ejecutar "docker compose up -d") -ne 0) { throw "Fallo docker compose up -d." }

    Write-Host ""
    Write-Host "Esperando a que los servicios queden healthy (maximo 8 minutos)..." -ForegroundColor Cyan
    $limite = (Get-Date).AddMinutes(8)
    do {
        Start-Sleep -Seconds 15
        $filas = cmd /c "docker compose ps --format {{.Service}}^|{{.State}}^|{{.Health}} 2>&1"
        $pendientes = @($filas | Where-Object { $_ -match "\|" -and ($_ -notmatch "\|running\|healthy$") -and ($_ -notmatch "\|running\|$") })
        Write-Host ("  {0}  pendientes: {1}" -f (Get-Date -Format HH:mm:ss), ($pendientes -join ", "))
    } while ($pendientes.Count -gt 0 -and (Get-Date) -lt $limite)

    Ejecutar "docker compose ps" | Out-Null
    if ($pendientes.Count -gt 0) { throw "Servicios sin quedar healthy: $($pendientes -join ', ')" }

    Ejecutar "docker compose exec -T airflow-scheduler airflow dags list-import-errors" | Out-Null
    Ejecutar "docker compose exec -T airflow-scheduler airflow dags list" | Out-Null
    $estado = "OK"
    Write-Host ""
    $puerto = (Get-Content (Join-Path $raiz ".env") | Where-Object { $_ -match "^AIRFLOW_PORT=(\d+)" } | ForEach-Object { $Matches[1] }) | Select-Object -First 1
    if (-not $puerto) { $puerto = "8080" }
    Write-Host "LISTO: Airflow en http://localhost:$puerto y MinIO en http://localhost:9001" -ForegroundColor Green
}
catch {
    Write-Host ""
    Write-Host "ERROR: $_" -ForegroundColor Red
}
finally {
    Stop-Transcript | Out-Null
    Set-Content -Path $marca -Value "$estado $(Get-Date -Format s)"
}
