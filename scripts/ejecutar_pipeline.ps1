# scripts/ejecutar_pipeline.ps1
# Corre el simulador del proveedor y el pipeline para uno o varios dias, uno
# detras de otro, y deja el detalle en logs_ejecucion/pipeline.log
#
# Uso:
#   scripts\ejecutar_pipeline.bat                     -> procesa AYER
#   powershell -ExecutionPolicy Bypass -File scripts\ejecutar_pipeline.ps1 -Hasta 2026-10-07 -Dias 8
#      -> procesa del 2026-09-30 al 2026-10-07 (8 dias: necesarios para el promedio de 7 dias)

param(
    [string]$Hasta = (Get-Date).AddDays(-1).ToString("yyyy-MM-dd"),
    [int]$Dias = 1,
    [int]$TimeoutMinutos = 30
)

$ErrorActionPreference = "Continue"
$raiz = Split-Path -Parent $PSScriptRoot
Set-Location $raiz
$dirLogs = Join-Path $raiz "logs_ejecucion"
New-Item -ItemType Directory -Force -Path $dirLogs | Out-Null
$marca = Join-Path $dirLogs "pipeline.estado"
Set-Content -Path $marca -Value "EN_CURSO $(Get-Date -Format s)"
Start-Transcript -Path (Join-Path $dirLogs "pipeline.log") -Force | Out-Null

function Airflow([string]$argumentos) {
    # Las comillas dobles se evitan a proposito (PowerShell 5.1 las altera al
    # pasarlas a programas externos): el JSON de conf se lee de un archivo.
    $salida = cmd /c "docker compose exec -T airflow-scheduler bash -c `"$argumentos`" 2>&1"
    return ,$salida
}

function Esperar-Corrida([string]$dagId, [string]$runId) {
    $limite = (Get-Date).AddMinutes($TimeoutMinutos)
    do {
        Start-Sleep -Seconds 20
        $json = (Airflow "airflow dags list-runs $dagId -o json 2>/dev/null") -join ""
        $estado = "desconocido"
        try {
            $corrida = ($json | ConvertFrom-Json) | Where-Object { $_.run_id -eq $runId } | Select-Object -First 1
            if ($corrida) { $estado = $corrida.state }
        } catch { }
        Write-Host ("  {0}  {1}: {2}" -f (Get-Date -Format HH:mm:ss), $dagId, $estado)
    } while ($estado -notin @("success", "failed") -and (Get-Date) -lt $limite)
    Write-Host "  Estado de las tareas:"
    Airflow "airflow tasks states-for-dag-run $dagId $runId -o table 2>/dev/null" | Out-Host
    return $estado
}

$resultado = "OK"
try {
    Airflow "airflow dags unpause simulador_envio_clinicas" | Out-Host
    Airflow "airflow dags unpause pipeline_salud_aire" | Out-Host

    $fin = [datetime]::ParseExact($Hasta, "yyyy-MM-dd", $null)
    for ($i = $Dias - 1; $i -ge 0; $i--) {
        $fecha = $fin.AddDays(-$i).ToString("yyyy-MM-dd")
        $sello = Get-Date -Format "HHmmss"
        Write-Host ""
        Write-Host "=== Dia $fecha ===" -ForegroundColor Cyan
        # conf en un archivo de la carpeta config/ (montada en /opt/airflow/config)
        Set-Content -Path (Join-Path $raiz "config\conf_fecha.json") -Value "{`"fecha_proceso`":`"$fecha`"}" -Encoding ASCII -NoNewline

        $runSim = "manual_sim_${fecha}_$sello"
        Airflow "airflow dags trigger simulador_envio_clinicas --run-id $runSim --conf `$(cat /opt/airflow/config/conf_fecha.json)" | Out-Host
        if ((Esperar-Corrida "simulador_envio_clinicas" $runSim) -ne "success") { throw "El simulador fallo para $fecha" }

        $runPipe = "manual_pipeline_${fecha}_$sello"
        Airflow "airflow dags trigger pipeline_salud_aire --run-id $runPipe --conf `$(cat /opt/airflow/config/conf_fecha.json)" | Out-Host
        if ((Esperar-Corrida "pipeline_salud_aire" $runPipe) -ne "success") { throw "El pipeline fallo para $fecha (ver tareas arriba y la UI de Airflow)" }
    }
    Write-Host ""
    Write-Host "LISTO: todos los dias procesados." -ForegroundColor Green
}
catch {
    $resultado = "ERROR"
    Write-Host ""
    Write-Host "ERROR: $_" -ForegroundColor Red
}
finally {
    Stop-Transcript | Out-Null
    Set-Content -Path $marca -Value "$resultado $(Get-Date -Format s)"
}
