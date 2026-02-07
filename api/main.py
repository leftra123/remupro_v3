"""
FastAPI application for RemuPro v3.

Provides a REST API for:
- File upload (Excel/CSV)
- BRP distribution processing
- Integrated processing (SEP + PIE + BRP)
- REM hour analysis
- Real-time progress via WebSocket

Run with:
    cd /Users/leftra123/Documents/RemunPRO/remupro_v3
    .venv/bin/uvicorn api.main:app --reload --port 8000
"""

import logging
import os
import sys
from contextlib import asynccontextmanager
from datetime import datetime
from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

# Ensure project root is on sys.path so processors, config, etc. can be imported
_project_root = str(Path(__file__).resolve().parent.parent)
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

from api.models import HealthResponse
from api.routes import upload, process, data, dashboard, preferences, anual
from api.ws import router as ws_router

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("api")

# ---------------------------------------------------------------------------
# Application
# ---------------------------------------------------------------------------

# Disable interactive API docs in production to reduce attack surface.
# Set REMUPRO_ENV=development to enable /docs and /redoc.
_is_dev = os.environ.get("REMUPRO_ENV", "").lower() == "development"


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("RemuPro API v3.0.0 starting up")
    logger.info("Project root: %s", _project_root)
    if _is_dev:
        logger.info("Docs available at /docs")
    yield
    logger.info("RemuPro API shutting down")
    # Cleanup temp files from all active sessions
    from api.session_store import store
    for session in list(store._sessions.values()):
        session.cleanup_files()
    import shutil
    if store.upload_dir.exists():
        shutil.rmtree(store.upload_dir, ignore_errors=True)
    logger.info("Temp files cleaned up")


app = FastAPI(
    title="RemuPro API",
    version="3.0.0",
    description=(
        "Backend API for RemuPro v3 - Procesamiento de remuneraciones educativas. "
        "Provides endpoints for uploading files, running BRP distribution, "
        "integrated processing, and REM analysis."
    ),
    docs_url="/docs" if _is_dev else None,
    redoc_url="/redoc" if _is_dev else None,
    lifespan=lifespan,
)

# ---------------------------------------------------------------------------
# CORS - allow local development front-ends
# ---------------------------------------------------------------------------

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:3000",
        "http://127.0.0.1:3000",
        "http://localhost:5173",
        "http://127.0.0.1:5173",
        "http://localhost:8501",  # Streamlit default
    ],
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
    allow_headers=["Content-Type", "Authorization", "X-Session-ID"],
)

# ---------------------------------------------------------------------------
# Include routers
# ---------------------------------------------------------------------------

app.include_router(upload.router)
app.include_router(process.router)
app.include_router(data.router)
app.include_router(dashboard.router)
app.include_router(preferences.router)
app.include_router(anual.router)
app.include_router(ws_router)

# ---------------------------------------------------------------------------
# Health check
# ---------------------------------------------------------------------------

@app.get("/api/health", response_model=HealthResponse, tags=["health"])
async def health_check():
    """
    Health check endpoint.

    Returns API status and lists available processor modules.
    """
    processors_available = []

    try:
        from processors.sep import SEPProcessor
        processors_available.append("SEPProcessor")
    except ImportError:
        pass

    try:
        from processors.pie import PIEProcessor
        processors_available.append("PIEProcessor")
    except ImportError:
        pass

    try:
        from processors.brp import BRPProcessor
        processors_available.append("BRPProcessor")
    except ImportError:
        pass

    try:
        from processors.integrado import IntegradoProcessor
        processors_available.append("IntegradoProcessor")
    except ImportError:
        pass

    try:
        from processors.rem import REMProcessor
        processors_available.append("REMProcessor")
    except ImportError:
        pass

    try:
        from processors.anual import AnualProcessor
        processors_available.append("AnualProcessor")
    except ImportError:
        pass

    try:
        from database import BRPRepository, ComparadorMeses
        processors_available.append("BRPRepository")
        processors_available.append("ComparadorMeses")
    except ImportError:
        pass

    return HealthResponse(
        status="ok",
        version="3.0.0",
        timestamp=datetime.now().isoformat(),
        processors_available=processors_available,
    )


