# scripts/asistente_local.ps1
# Ejecuta, a pedido, SOLO los scripts de operacion de este proyecto.
# Revisa cada 5 segundos el archivo logs_ejecucion\orden.txt; si encuentra una
# orden valida la ejecuta y borra el archivo. Cualquier otra cosa se ignora.
#
# Ordenes validas (una por archivo):
#   levantar
#   diagnostico
#   pipeline AAAA-MM-DD N      (procesa N dias terminando en esa fecha)
#   detener                    (docker compose down, conserva los datos)
#
# Se cierra con Ctrl+C o cerrando la ventana.

$raiz = Split-Path -Parent $PSScriptRoot
$dirLogs = Join-Path $raiz "logs_ejecucion"
New-Item -ItemType Directory -Force -Path $dirLogs | Out-Null
$orden = Join-Path $dirLogs "orden.txt"
$latido = Join-Path $dirLogs "asistente.estado"

Write-Host "Asistente local activo en $raiz" -ForegroundColor Green
Write-Host "Esperando ordenes en logs_ejecucion\orden.txt (Ctrl+C para salir)..."

while ($true) {
    Set-Content -Path $latido -Value "ACTIVO $(Get-Date -Format s)"
    if (Test-Path $orden) {
        $texto = (Get-Content $orden -Raw).Trim()
        Remove-Item $orden -Force
        Write-Host ""
        Write-Host "[$(Get-Date -Format HH:mm:ss)] Orden recibida: $texto" -ForegroundColor Cyan
        Set-Content -Path $latido -Value "EJECUTANDO $texto $(Get-Date -Format s)"
        switch -Regex ($texto) {
            '^levantar$'    { & powershell.exe -NoProfile -ExecutionPolicy Bypass -File (Join-Path $PSScriptRoot "levantar.ps1") }
            '^diagnostico$' { & powershell.exe -NoProfile -ExecutionPolicy Bypass -File (Join-Path $PSScriptRoot "diagnostico.ps1") }
            '^pipeline (\d{4}-\d{2}-\d{2}) (\d{1,2})$' {
                & powershell.exe -NoProfile -ExecutionPolicy Bypass -File (Join-Path $PSScriptRoot "ejecutar_pipeline.ps1") -Hasta $Matches[1] -Dias ([int]$Matches[2])
            }
            '^detener$' {
                Set-Location $raiz
                cmd /c "docker compose down 2>&1" | Out-File (Join-Path $dirLogs "detener.log") -Encoding utf8
            }
            default { Write-Host "Orden no reconocida; se ignora." -ForegroundColor Yellow }
        }
        Write-Host "[$(Get-Date -Format HH:mm:ss)] Orden terminada." -ForegroundColor Green
    }
    Start-Sleep -Seconds 5
}
