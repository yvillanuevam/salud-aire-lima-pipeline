@echo off
REM Doble clic: corre el simulador y el pipeline para los 8 dias anteriores a hoy
REM (8 dias = historia suficiente para el promedio de 7 dias y la alerta).
cd /d "%~dp0.."
powershell.exe -NoProfile -ExecutionPolicy Bypass -File "%~dp0ejecutar_pipeline.ps1" -Dias 8
echo.
echo Terminado. Revisa logs_ejecucion\pipeline.log
pause
