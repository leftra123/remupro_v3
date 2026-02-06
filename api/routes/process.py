"""
Processing endpoints.

Launches BRP, Integrado, and REM processing as background tasks
and tracks progress via session state. Clients can poll for status
or subscribe to the WebSocket for real-time progress updates.
"""

import asyncio
import logging
import tempfile
from pathlib import Path
from typing import Callable

from fastapi import APIRouter, BackgroundTasks, HTTPException

from api.models import (
    BRPProcessRequest,
    IntegradoProcessRequest,
    ProcessingStatus,
    ProcessStartResponse,
    REMProcessRequest,
)
from api.session_store import SessionData, store
from api.ws import notify_progress

logger = logging.getLogger("api.process")

router = APIRouter(prefix="/api/process", tags=["process"])


def _safe_error_message(exc: Exception) -> str:
    """Return a sanitized error message suitable for API responses.

    Strips file paths and internal details that could leak server information.
    The full exception is still available in server logs via logger.exception().
    """
    from processors.base import ProcessorError, ColumnMissingError, FileValidationError

    # Known domain errors are safe to expose
    if isinstance(exc, (ProcessorError, ColumnMissingError, FileValidationError)):
        return str(exc)
    # Generic errors: expose only the exception type, not the message
    return f"Processing failed ({type(exc).__name__}). Check server logs for details."


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _resolve_file(file_id: str, label: str) -> Path:
    """Resolve a file_id to a Path, raising HTTP 404 if missing."""
    path = store.resolve_file(file_id)
    if path is None or not path.exists():
        raise HTTPException(
            status_code=404,
            detail=f"File not found for {label} (file_id={file_id}). Upload the file first.",
        )
    return path


def _make_progress_callback(session: SessionData) -> Callable[[int, str], None]:
    """Create a progress callback that updates session state and notifies WebSocket."""

    def callback(value: int, message: str) -> None:
        session.progress = value
        session.progress_message = message
        # Fire-and-forget WebSocket notification (best-effort)
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                loop.create_task(notify_progress(session.session_id, value, message))
        except RuntimeError:
            pass

    return callback


# ---------------------------------------------------------------------------
# BRP Processing (pre-processed files)
# ---------------------------------------------------------------------------

def _run_brp(session: SessionData, web_path: Path, sep_path: Path, pie_path: Path) -> None:
    """Background task: run BRPProcessor."""
    import sys, os
    # Ensure project root is on sys.path so processors can be imported
    project_root = str(Path(__file__).resolve().parent.parent.parent)
    if project_root not in sys.path:
        sys.path.insert(0, project_root)

    from processors.brp import BRPProcessor
    from reports.audit_log import AuditLog

    session.status = ProcessingStatus.PROCESSING
    session.process_type = "brp"

    try:
        # Output file
        fd, out_path = tempfile.mkstemp(suffix="_brp_result.xlsx", dir=str(store.upload_dir))
        os.close(fd)
        output_path = Path(out_path)

        processor = BRPProcessor()
        progress_cb = _make_progress_callback(session)

        processor.process_file(
            web_sostenedor_path=web_path,
            sep_procesado_path=sep_path,
            pie_procesado_path=pie_path,
            output_path=output_path,
            progress_callback=progress_cb,
        )

        # Read result sheets
        import pandas as pd
        session.result_df = pd.read_excel(output_path, sheet_name="BRP_DISTRIBUIDO", engine="openpyxl")
        try:
            session.multi_establishment_df = pd.read_excel(
                output_path, sheet_name="MULTI_ESTABLECIMIENTO", engine="openpyxl"
            )
        except Exception:
            session.multi_establishment_df = None

        session.output_path = output_path
        session.column_alerts = processor.get_column_alerts()
        session.docentes_revisar = processor.docentes_revisar
        session.summary = _build_summary(session.result_df, len(processor.docentes_revisar))

        session.set_completed()
        logger.info("BRP processing completed for session %s", session.session_id)

    except Exception as exc:
        logger.exception("BRP processing failed for session %s", session.session_id)
        session.set_failed(_safe_error_message(exc))


@router.post("/brp", response_model=ProcessStartResponse)
async def process_brp(request: BRPProcessRequest, background_tasks: BackgroundTasks):
    """
    Start BRP distribution processing.

    Requires three pre-processed file IDs:
    - web_file_id: MINEDUC web_sostenedor file
    - sep_file_id: Processed SEP file
    - pie_file_id: Processed PIE file
    """
    web_path = _resolve_file(request.web_file_id, "web_sostenedor")
    sep_path = _resolve_file(request.sep_file_id, "SEP procesado")
    pie_path = _resolve_file(request.pie_file_id, "PIE procesado")

    session = store.create_session()
    # Copy file references from upload sessions
    for fid in [request.web_file_id, request.sep_file_id, request.pie_file_id]:
        for s in store._sessions.values():
            if fid in s.files:
                session.files[fid] = s.files[fid]

    session.status = ProcessingStatus.PENDING
    session.process_type = "brp"

    background_tasks.add_task(_run_brp, session, web_path, sep_path, pie_path)

    return ProcessStartResponse(
        session_id=session.session_id,
        status=session.status,
        message="BRP processing started. Poll GET /api/results/{session_id} for status.",
    )


# ---------------------------------------------------------------------------
# Integrado Processing (raw files)
# ---------------------------------------------------------------------------

def _run_integrado(
    session: SessionData, sep_path: Path, pie_path: Path, web_path: Path
) -> None:
    """Background task: run IntegradoProcessor."""
    import sys, os
    project_root = str(Path(__file__).resolve().parent.parent.parent)
    if project_root not in sys.path:
        sys.path.insert(0, project_root)

    from processors.integrado import IntegradoProcessor

    session.status = ProcessingStatus.PROCESSING
    session.process_type = "integrado"

    try:
        fd, out_path = tempfile.mkstemp(suffix="_integrado_result.xlsx", dir=str(store.upload_dir))
        os.close(fd)
        output_path = Path(out_path)

        processor = IntegradoProcessor()
        progress_cb = _make_progress_callback(session)

        df_result, audit = processor.process_all(
            sep_bruto_path=sep_path,
            pie_bruto_path=pie_path,
            web_sostenedor_path=web_path,
            output_path=output_path,
            progress_callback=progress_cb,
            keep_intermediates=True,
        )

        session.result_df = df_result
        session.output_path = output_path
        session.column_alerts = processor.brp_processor.get_column_alerts()
        session.docentes_revisar = processor.get_docentes_revisar()
        session.summary = _build_summary(df_result, len(session.docentes_revisar))

        # Store intermediate file paths for individual downloads
        intermediate_paths = processor.get_intermediate_paths()
        if len(intermediate_paths) >= 1:
            session.sep_output_path = intermediate_paths[0]
        if len(intermediate_paths) >= 2:
            session.pie_output_path = intermediate_paths[1]

        # Store audit log entries
        session.audit_entries = [entry.to_dict() for entry in audit.entries]

        # Read multi-establishment sheet if available
        try:
            import pandas as pd
            session.multi_establishment_df = pd.read_excel(
                output_path, sheet_name="MULTI_ESTABLECIMIENTO", engine="openpyxl"
            )
        except Exception:
            session.multi_establishment_df = None

        # Auto-save to database if month is specified
        if session.mes:
            try:
                from database.repository import BRPRepository
                repo = BRPRepository()
                repo.guardar_procesamiento(session.mes, df_result)
                logger.info("Auto-saved processing to DB for month %s", session.mes)
            except Exception as db_exc:
                logger.warning("Failed to auto-save to DB: %s", db_exc)

        session.set_completed()
        logger.info("Integrado processing completed for session %s", session.session_id)

    except Exception as exc:
        logger.exception("Integrado processing failed for session %s", session.session_id)
        session.set_failed(_safe_error_message(exc))


@router.post("/integrado", response_model=ProcessStartResponse)
async def process_integrado(
    request: IntegradoProcessRequest, background_tasks: BackgroundTasks
):
    """
    Start integrated processing (SEP raw + PIE raw + web_sostenedor).

    This orchestrates SEP, PIE, and BRP processors in sequence.
    """
    sep_path = _resolve_file(request.sep_bruto_file_id, "SEP bruto")
    pie_path = _resolve_file(request.pie_bruto_file_id, "PIE bruto")
    web_path = _resolve_file(request.web_file_id, "web_sostenedor")

    session = store.create_session()
    for fid in [request.sep_bruto_file_id, request.pie_bruto_file_id, request.web_file_id]:
        for s in store._sessions.values():
            if fid in s.files:
                session.files[fid] = s.files[fid]

    session.status = ProcessingStatus.PENDING
    session.process_type = "integrado"
    session.mes = request.mes

    background_tasks.add_task(_run_integrado, session, sep_path, pie_path, web_path)

    return ProcessStartResponse(
        session_id=session.session_id,
        status=session.status,
        message="Integrado processing started. Poll GET /api/results/{session_id} for status.",
    )


# ---------------------------------------------------------------------------
# REM Processing
# ---------------------------------------------------------------------------

def _run_rem(session: SessionData, rem_path: Path) -> None:
    """Background task: run REMProcessor."""
    import sys
    project_root = str(Path(__file__).resolve().parent.parent.parent)
    if project_root not in sys.path:
        sys.path.insert(0, project_root)

    from processors.rem import REMProcessor

    session.status = ProcessingStatus.PROCESSING
    session.process_type = "rem"

    try:
        processor = REMProcessor()
        progress_cb = _make_progress_callback(session)

        progress_cb(10, "Cargando archivo REM...")
        df_resumen, df_detalle, alertas = processor.process(rem_path)
        progress_cb(80, "Procesamiento REM completado")

        session.rem_resumen_df = df_resumen
        session.result_df = df_detalle
        session.rem_alertas = alertas

        # Build a lightweight summary for REM
        session.summary = {
            "total_personas": len(df_resumen),
            "exceden_44": int(df_resumen["EXCEDE"].sum()) if "EXCEDE" in df_resumen.columns else 0,
        }

        progress_cb(100, "REM completado")
        session.set_completed()
        logger.info("REM processing completed for session %s", session.session_id)

    except Exception as exc:
        logger.exception("REM processing failed for session %s", session.session_id)
        session.set_failed(_safe_error_message(exc))


@router.post("/rem", response_model=ProcessStartResponse)
async def process_rem(request: REMProcessRequest, background_tasks: BackgroundTasks):
    """
    Start REM file processing for hour analysis.
    """
    rem_path = _resolve_file(request.rem_file_id, "REM")

    session = store.create_session()
    for s in store._sessions.values():
        if request.rem_file_id in s.files:
            session.files[request.rem_file_id] = s.files[request.rem_file_id]

    session.status = ProcessingStatus.PENDING
    session.process_type = "rem"

    background_tasks.add_task(_run_rem, session, rem_path)

    return ProcessStartResponse(
        session_id=session.session_id,
        status=session.status,
        message="REM processing started. Poll GET /api/results/{session_id} for status.",
    )


# ---------------------------------------------------------------------------
# Summary builder
# ---------------------------------------------------------------------------

def _build_summary(df, casos_revision: int = 0) -> dict:
    """Build a general summary dict from a BRP result DataFrame."""
    import pandas as pd
    import numpy as np

    if df is None or df.empty:
        return {}

    def safe_sum(col: str) -> int:
        if col in df.columns:
            return int(df[col].sum())
        return 0

    brp_sep = safe_sum("BRP_SEP")
    brp_pie = safe_sum("BRP_PIE")
    brp_normal = safe_sum("BRP_NORMAL")
    brp_total = brp_sep + brp_pie + brp_normal

    # Detect RUT column
    rut_col = None
    for col in df.columns:
        if col == "RUT_NORM" or "rut" in col.lower():
            rut_col = col
            break
    total_docentes = int(df[rut_col].nunique()) if rut_col else len(df)

    # Detect RBD column
    rbd_col = None
    for col in df.columns:
        if "rbd" in col.lower():
            rbd_col = col
            break
    total_establecimientos = int(df[rbd_col].nunique()) if rbd_col else 0

    pct_sep = round(100 * brp_sep / brp_total, 1) if brp_total > 0 else 0.0
    pct_pie = round(100 * brp_pie / brp_total, 1) if brp_total > 0 else 0.0
    pct_normal = round(100 * brp_normal / brp_total, 1) if brp_total > 0 else 0.0

    daem_total = safe_sum("TOTAL_DAEM_SEP") + safe_sum("TOTAL_DAEM_PIE") + safe_sum("TOTAL_DAEM_NORMAL")
    cpeip_total = safe_sum("TOTAL_CPEIP_SEP") + safe_sum("TOTAL_CPEIP_PIE") + safe_sum("TOTAL_CPEIP_NORMAL")

    recon_total = (
        safe_sum("BRP_RECONOCIMIENTO_SEP")
        + safe_sum("BRP_RECONOCIMIENTO_PIE")
        + safe_sum("BRP_RECONOCIMIENTO_NORMAL")
    )
    tramo_total = (
        safe_sum("BRP_TRAMO_SEP")
        + safe_sum("BRP_TRAMO_PIE")
        + safe_sum("BRP_TRAMO_NORMAL")
    )

    return {
        "total_docentes": total_docentes,
        "total_establecimientos": total_establecimientos,
        "brp_total": brp_total,
        "brp_sep": brp_sep,
        "brp_pie": brp_pie,
        "brp_normal": brp_normal,
        "pct_sep": pct_sep,
        "pct_pie": pct_pie,
        "pct_normal": pct_normal,
        "daem_total": daem_total,
        "cpeip_total": cpeip_total,
        "reconocimiento_total": recon_total,
        "tramo_total": tramo_total,
        "casos_revision": casos_revision,
    }
