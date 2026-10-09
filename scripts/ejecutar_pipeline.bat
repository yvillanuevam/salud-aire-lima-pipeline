@echo off
REM Doble clic para ejecutar scripts\ejecutar_pipeline.ps1 (el detalle queda en logs_ejecucion\)
cd /d "%~dp0.."
powershell.exe -NoProfile -ExecutionPolicy Bypass -File "%~dp0ejecutar_pipeline.ps1" %*
echo.
echo Terminado. Revisa logs_ejecucion\ejecutar_pipeline.log
pause
