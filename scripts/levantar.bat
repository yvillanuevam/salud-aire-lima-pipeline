@echo off
REM Doble clic para ejecutar scripts\levantar.ps1 (el detalle queda en logs_ejecucion\)
cd /d "%~dp0.."
powershell.exe -NoProfile -ExecutionPolicy Bypass -File "%~dp0levantar.ps1" %*
echo.
echo Terminado. Revisa logs_ejecucion\levantar.log
pause
