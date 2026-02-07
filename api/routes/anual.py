"""
API routes for annual liquidation processing and dashboard.
"""

import logging
import threading
from datetime import datetime
from pathlib import Path
from typing import Optional

from fastapi import APIRouter, HTTPException, Header, Query

from api.models import (
    AnualProcessRequest,
    AnualSummaryResponse,
    AnualTrendPoint,
    AnualTrendsResponse,
    AnualYearsResponse,
    AnualSchoolsResponse,
    AnualMultiEstablishmentResponse,
    ProcessStartResponse,
    ProcessingStatus,
    TeacherSearchResponse,
)
from api.session_store import store

logger = logging.getLogger("api.anual")

router = APIRouter(prefix="/api", tags=["anual"])


def _get_anual_repo():
    from database import AnualRepository
    return AnualRepository()


# ---------------------------------------------------------------------------
# Process
# ---------------------------------------------------------------------------

@router.post("/process/anual", response_model=ProcessStartResponse)
async def process_anual(
    request: AnualProcessRequest,
    x_session_id: Optional[str] = Header(None),
):
    """Procesa archivo anual de liquidaciones consolidadas."""
    session = store.get_or_create_session(x_session_id)
    file_path = store.resolve_file(request.anual_file_id)
    if not file_path:
        raise HTTPException(status_code=404, detail="Archivo anual no encontrado")

    session.status = ProcessingStatus.PROCESSING
    session.process_type = "anual"
    session.progress = 0
    session.progress_message = "Iniciando procesamiento anual..."

    def _run():
        try:
            from processors.anual import AnualProcessor

            session.progress = 20
            session.progress_message = "Cargando archivo anual..."

            processor = AnualProcessor()
            df_mensual, df_resumen, df_escuelas, alertas = processor.process(file_path)

            session.progress = 60
            session.progress_message = "Guardando en base de datos..."

            # Detectar año
            anio = request.anio
            if not anio and 'MES' in df_mensual.columns:
                meses = df_mensual['MES'].dropna().unique()
                for m in meses:
                    if len(str(m)) >= 4:
                        try:
                            anio = int(str(m)[:4])
                            break
                        except ValueError:
                            continue
            if not anio:
                anio = datetime.now().year

            # Guardar en DB
            repo = _get_anual_repo()
            repo.guardar_procesamiento_anual(anio, df_mensual)

            session.progress = 80
            session.progress_message = "Preparando resultados..."

            # Guardar en sesión
            session.anual_mensual_df = df_mensual
            session.anual_resumen_df = df_resumen
            session.anual_escuelas_df = df_escuelas
            session.anual_alertas = alertas

            session.summary = {
                'anio': anio,
                'total_docentes': int(df_mensual['RUT_NORM'].nunique()) if 'RUT_NORM' in df_mensual.columns else 0,
                'total_establecimientos': int(df_mensual['RBD'].nunique()) if 'RBD' in df_mensual.columns else 0,
                'brp_total_anual': float(df_mensual['BRP'].sum()) if 'BRP' in df_mensual.columns else 0,
                'haberes_total_anual': float(df_mensual['TOTAL_HABERES'].sum()) if 'TOTAL_HABERES' in df_mensual.columns else 0,
                'liquido_total_anual': float(df_mensual['LIQUIDO_NETO'].sum()) if 'LIQUIDO_NETO' in df_mensual.columns else 0,
            }

            session.set_completed()
            logger.info(f"Procesamiento anual {anio} completado: {len(df_mensual)} registros")

        except Exception as e:
            logger.exception("Error en procesamiento anual")
            session.set_failed("Error al procesar archivo anual. Verifique el formato del archivo.")

    thread = threading.Thread(target=_run, daemon=True)
    thread.start()

    return ProcessStartResponse(
        session_id=session.session_id,
        status=ProcessingStatus.PROCESSING,
        message="Procesamiento anual iniciado",
    )


# ---------------------------------------------------------------------------
# Dashboard anual
# ---------------------------------------------------------------------------

@router.get("/dashboard/anual/years", response_model=AnualYearsResponse)
async def get_anual_years():
    """Años disponibles con procesamiento anual."""
    repo = _get_anual_repo()
    years = repo.obtener_anios_disponibles()
    return AnualYearsResponse(years=years)


@router.get("/dashboard/anual/summary/{anio}", response_model=AnualSummaryResponse)
async def get_anual_summary(anio: int):
    """Resumen del año."""
    repo = _get_anual_repo()
    resumen = repo.obtener_resumen_anual(anio)
    if not resumen:
        raise HTTPException(status_code=404, detail=f"No hay datos para el año {anio}")
    return AnualSummaryResponse(**resumen)


@router.get("/dashboard/anual/trends/{anio}", response_model=AnualTrendsResponse)
async def get_anual_trends(anio: int):
    """Tendencias mensuales dentro del año."""
    repo = _get_anual_repo()
    trends = repo.obtener_tendencias_mensuales(anio)
    return AnualTrendsResponse(
        anio=anio,
        trends=[AnualTrendPoint(**t) for t in trends],
    )


@router.get("/dashboard/anual/teachers/{anio}", response_model=TeacherSearchResponse)
async def search_anual_teachers(
    anio: int,
    q: str = Query("", description="Buscar por RUT o nombre"),
    rbd: str = Query("", description="Filtrar por RBD"),
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
):
    """Búsqueda de docentes en procesamiento anual."""
    repo = _get_anual_repo()
    result = repo.buscar_docentes_anual(anio, query=q, rbd=rbd, limit=limit, offset=offset)
    return TeacherSearchResponse(**result)


@router.get("/dashboard/anual/schools/{anio}", response_model=AnualSchoolsResponse)
async def get_anual_schools(anio: int):
    """Escuelas con agregados para un año."""
    repo = _get_anual_repo()
    escuelas = repo.obtener_escuelas_anual(anio)
    return AnualSchoolsResponse(escuelas=escuelas)


@router.get("/dashboard/anual/multi-establishment/{anio}", response_model=AnualMultiEstablishmentResponse)
async def get_anual_multi_establishment(anio: int):
    """Docentes en múltiples establecimientos durante el año."""
    repo = _get_anual_repo()
    docentes = repo.obtener_multi_establecimiento_anual(anio)
    return AnualMultiEstablishmentResponse(total=len(docentes), docentes=docentes)
