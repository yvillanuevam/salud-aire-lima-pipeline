@echo off
REM Deja corriendo el asistente local: ejecuta a pedido los scripts del proyecto.
cd /d "%~dp0.."
powershell.exe -NoProfile -ExecutionPolicy Bypass -File "%~dp0asistente_local.ps1"
pause
