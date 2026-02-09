#!/bin/bash
# RemuPro v2.4 - Launcher para Mac/Linux

echo ""
echo "  ╔═══════════════════════════════════════════════╗"
echo "  ║         📊 RemuPro v2.4                       ║"
echo "  ║   Remuneraciones Educativas                   ║"
echo "  ╚═══════════════════════════════════════════════╝"
echo ""

cd "$(dirname "$0")"

# Verificar Python
if ! command -v python3 &> /dev/null; then
    echo "  ❌ Python3 no encontrado."
    echo "     Instala con: brew install python3"
    exit 1
fi

# Crear venv si no existe
if [ ! -d "venv" ]; then
    echo "  ⚙️  Creando entorno virtual..."
    python3 -m venv venv
fi

# Activar venv
source venv/bin/activate

# Instalar dependencias si es necesario
if [ ! -f ".deps_installed" ]; then
    echo "  ⚙️  Instalando dependencias..."
    pip install -q -r requirements.txt
    touch .deps_installed
    echo "  ✅ Dependencias instaladas"
    echo ""
fi

# Mostrar IP local
IP=$(python3 -c "import socket; print(socket.gethostbyname(socket.gethostname()))" 2>/dev/null || echo "127.0.0.1")
echo "  🌐 Acceso en red local: http://${IP}:8501"
echo "  🏠 Acceso local:       http://localhost:8501"
echo ""
echo "  🚀 Iniciando aplicación..."
echo ""
echo "  ────────────────────────────────────────────────"
echo "   Para CERRAR: Ctrl+C"
echo "  ────────────────────────────────────────────────"
echo ""

streamlit run app.py --server.address=0.0.0.0
