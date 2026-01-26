@echo off
chcp 65001 >nul
title RemuPro v2.1

cd /d "%~dp0"

echo.
echo  ╔═══════════════════════════════════════════════╗
echo  ║         📊 RemuPro v2.1                       ║
echo  ║   Remuneraciones Educativas                   ║
echo  ╚═══════════════════════════════════════════════╝
echo.

:: Verificar Python
python --version >nul 2>&1
if errorlevel 1 (
    echo  ❌ Python no encontrado.
    echo     Descarga desde: https://python.org
    echo     Marca "Add Python to PATH" al instalar.
    echo.
    pause
    exit /b 1
)

:: Instalar dependencias si es necesario
if not exist ".deps_installed" (
    echo  ⚙️  Instalando dependencias...
    pip install -q -r requirements.txt
    echo. > .deps_installed
    echo  ✅ Dependencias instaladas
    echo.
)

echo  🚀 Iniciando aplicación...
echo  📍 Abriendo en el navegador...
echo.
echo  ────────────────────────────────────────────────
echo   Para CERRAR: Cierra esta ventana
echo  ────────────────────────────────────────────────
echo.

streamlit run app.py --server.headless=true --browser.gatherUsageStats=false

pause
