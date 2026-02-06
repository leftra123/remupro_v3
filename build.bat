@echo off
echo === RemuPro v3 - Build ===
echo.

set ROOT_DIR=%~dp0
cd /d "%ROOT_DIR%"

:: Step 1: Build Next.js dashboard (static export)
echo [1/3] Construyendo dashboard Next.js...
cd dashboard
call npm install
call npm run build
echo       Dashboard exportado a dashboard\out\

:: Step 2: Install Electron dependencies
echo [2/3] Instalando dependencias Electron...
cd /d "%ROOT_DIR%\electron"
call npm install
echo       Dependencias Electron instaladas

:: Step 3: Build Electron app
echo [3/3] Construyendo aplicacion Electron...
call npm run build:win

echo.
echo === Build completado ===
echo Instalador generado en dist\
pause
