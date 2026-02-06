#!/bin/bash
# Setup script for RemuPro Dashboard
# Run this once after cloning to install all dependencies

echo "=== RemuPro Dashboard Setup ==="
echo ""

cd "$(dirname "$0")"

echo "[1/2] Installing npm dependencies..."
npm install

echo ""
echo "[2/2] Setup complete!"
echo ""
echo "To start the development server:"
echo "  cd dashboard && npm run dev"
echo ""
echo "The dashboard will be available at http://localhost:3000"
echo "Make sure the FastAPI backend is running at http://localhost:8000"
echo ""
