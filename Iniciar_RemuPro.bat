@echo off
chcp 65001 >nul
title RemuPro v2.4

cd /d "%~dp0"

echo.
echo  ╔═══════════════════════════════════════════════╗
echo  ║         📊 RemuPro v2.4                       ║
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

:: Mostrar IP local para acceso en red
echo  🌐 Acceso en red local:
for /f "tokens=2 delims=:" %%a in ('ipconfig ^| findstr /c:"IPv4"') do (
    echo     http://%%a:8501
)
echo.

echo  🚀 Iniciando aplicación...
echo  📍 Abriendo en el navegador...
echo.
echo  ────────────────────────────────────────────────
echo   Para CERRAR: Cierra esta ventana
echo  ────────────────────────────────────────────────
echo.

streamlit run app.py --server.headless=true --server.address=0.0.0.0 --browser.gatherUsageStats=false

pause
