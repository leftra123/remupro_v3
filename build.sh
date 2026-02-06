#!/bin/bash
set -e

echo "=== RemuPro v3 - Build ==="
echo ""

ROOT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$ROOT_DIR"

# Step 1: Build Next.js dashboard (static export)
echo "[1/3] Construyendo dashboard Next.js..."
cd dashboard
npm install
npm run build
echo "      Dashboard exportado a dashboard/out/"

# Step 2: Install Electron dependencies
echo "[2/3] Instalando dependencias Electron..."
cd "$ROOT_DIR/electron"
npm install
echo "      Dependencias Electron instaladas"

# Step 3: Build Electron app
echo "[3/3] Construyendo aplicacion Electron..."
if [[ "$OSTYPE" == "msys" ]] || [[ "$OSTYPE" == "win32" ]]; then
  npm run build:win
elif [[ "$OSTYPE" == "darwin"* ]]; then
  npm run build:mac
else
  npm run build:linux
fi

echo ""
echo "=== Build completado ==="
echo "Instalador generado en dist/"
