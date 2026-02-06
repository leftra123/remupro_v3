"""
Data retrieval endpoints.

Provides access to processing results, audit logs, multi-establishment
breakdowns, and Excel file downloads.
"""

import math
from pathlib import Path
from typing import Optional

import pandas as pd
from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import FileResponse

from api.models import (
    AuditEntryResponse,
    AuditLogResponse,
    GeneralSummary,
    MultiEstablishmentResponse,
    ProcessingStatus,
    REMResultsResponse,
    ResultsResponse,
)
from api.session_store import store

router = APIRouter(prefix="/api/results", tags=["results"])


def _get_session_or_404(session_id: str):
    session = store.get_session(session_id)
    if session is None:
        raise HTTPException(status_code=404, detail=f"Session not found: {session_id}")
    return session


def _df_to_records(df: Optional[pd.DataFrame], limit: int = 50, offset: int = 0):
    """Convert a DataFrame slice to a list of JSON-safe dicts."""
    if df is None or df.empty:
        return []
    subset = df.iloc[offset : offset + limit]
    # Replace NaN/inf with None for JSON serialization
    records = subset.where(subset.notna(), None).to_dict(orient="records")
    # Ensure all values are JSON-serializable
    clean = []
    for row in records:
        clean_row = {}
        for k, v in row.items():
            if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
                clean_row[k] = None
            else:
                clean_row[k] = v
        clean.append(clean_row)
    return clean


# ---------------------------------------------------------------------------
# Main results endpoint
# ---------------------------------------------------------------------------

@router.get("/{session_id}", response_model=ResultsResponse)
async def get_results(
    session_id: str,
    limit: int = Query(50, ge=1, le=1000, description="Max rows in data preview"),
    offset: int = Query(0, ge=0, description="Row offset for pagination"),
):
    """
    Get processing results for a session.

    Returns status, summary, alerts, revision cases, and a paginated
    preview of the result data.
    """
    session = _get_session_or_404(session_id)

    # For REM sessions, redirect to the REM-specific shape
    if session.process_type == "rem" and session.status == ProcessingStatus.COMPLETED:
        return ResultsResponse(
            session_id=session.session_id,
            status=session.status,
            process_type=session.process_type,
            created_at=session.created_at.isoformat(),
            completed_at=session.completed_at.isoformat() if session.completed_at else None,
            summary=None,
            data_preview=_df_to_records(session.rem_resumen_df, limit, offset),
            total_rows=len(session.rem_resumen_df) if session.rem_resumen_df is not None else 0,
            error=session.error,
        )

    # Build summary model
    summary_model = None
    if session.summary:
        summary_model = GeneralSummary(**session.summary)

    return ResultsResponse(
        session_id=session.session_id,
        status=session.status,
        process_type=session.process_type,
        created_at=session.created_at.isoformat(),
        completed_at=session.completed_at.isoformat() if session.completed_at else None,
        summary=summary_model,
        column_alerts=session.column_alerts,
        docentes_revisar=session.docentes_revisar,
        data_preview=_df_to_records(session.result_df, limit, offset),
        total_rows=len(session.result_df) if session.result_df is not None else 0,
        error=session.error,
    )


# ---------------------------------------------------------------------------
# Multi-establishment breakdown
# ---------------------------------------------------------------------------

@router.get("/{session_id}/multi-establishment", response_model=MultiEstablishmentResponse)
async def get_multi_establishment(session_id: str):
    """
    Get multi-establishment breakdown for teachers working in 2+ schools.
    """
    session = _get_session_or_404(session_id)

    if session.status != ProcessingStatus.COMPLETED:
        raise HTTPException(
            status_code=409,
            detail=f"Processing not yet completed (status={session.status.value})",
        )

    df = session.multi_establishment_df
    if df is None or df.empty:
        return MultiEstablishmentResponse(
            session_id=session_id,
            total_docentes_multi=0,
            entries=[],
        )

    # Count unique multi-establishment teachers
    rut_col = "RUT" if "RUT" in df.columns else None
    total_multi = int(df[rut_col].nunique()) if rut_col else 0

    return MultiEstablishmentResponse(
        session_id=session_id,
        total_docentes_multi=total_multi,
        entries=_df_to_records(df, limit=5000),
    )


# ---------------------------------------------------------------------------
# Audit log
# ---------------------------------------------------------------------------

@router.get("/{session_id}/audit-log", response_model=AuditLogResponse)
async def get_audit_log(
    session_id: str,
    nivel: Optional[str] = Query(None, description="Filter by level: INFO, WARNING, ERROR"),
    tipo: Optional[str] = Query(None, description="Filter by event type"),
):
    """
    Get audit log entries for a session, with optional filters.
    """
    session = _get_session_or_404(session_id)

    entries = session.audit_entries
    if not entries:
        return AuditLogResponse(total=0, entries=[], summary={})

    # Apply filters
    filtered = entries
    if nivel:
        nivel_upper = nivel.upper()
        filtered = [e for e in filtered if e.get("nivel", "").upper() == nivel_upper]
    if tipo:
        filtered = [e for e in filtered if e.get("tipo", "") == tipo]

    # Build response entries
    response_entries = []
    for e in filtered:
        # Separate known fields from extra data
        datos = {k: v for k, v in e.items() if k not in ("timestamp", "nivel", "tipo", "mensaje")}
        response_entries.append(
            AuditEntryResponse(
                timestamp=e.get("timestamp", ""),
                nivel=e.get("nivel", ""),
                tipo=e.get("tipo", ""),
                mensaje=e.get("mensaje", ""),
                datos=datos,
            )
        )

    # Summary counts
    niveles = {}
    tipos = {}
    for e in entries:
        n = e.get("nivel", "")
        t = e.get("tipo", "")
        niveles[n] = niveles.get(n, 0) + 1
        tipos[t] = tipos.get(t, 0) + 1

    summary = {
        "total": len(entries),
        "por_nivel": niveles,
        "por_tipo": tipos,
        "errores": niveles.get("ERROR", 0),
        "advertencias": niveles.get("WARNING", 0),
    }

    return AuditLogResponse(
        total=len(filtered),
        entries=response_entries,
        summary=summary,
    )


# ---------------------------------------------------------------------------
# Excel download
# ---------------------------------------------------------------------------

@router.get("/{session_id}/download/excel")
async def download_excel(session_id: str):
    """
    Download the processing result as an Excel file.
    """
    session = _get_session_or_404(session_id)

    if session.status != ProcessingStatus.COMPLETED:
        raise HTTPException(
            status_code=409,
            detail=f"Processing not yet completed (status={session.status.value})",
        )

    if session.output_path is None or not session.output_path.exists():
        raise HTTPException(status_code=404, detail="Output file not found")

    filename = f"remupro_{session.process_type}_{session_id[:8]}.xlsx"

    return FileResponse(
        path=str(session.output_path),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        filename=filename,
    )


# ---------------------------------------------------------------------------
# Individual file downloads
# ---------------------------------------------------------------------------

@router.get("/{session_id}/download/sep")
async def download_sep(session_id: str):
    """Download the processed SEP file."""
    session = _get_session_or_404(session_id)
    if session.status != ProcessingStatus.COMPLETED:
        raise HTTPException(status_code=409, detail="Processing not completed")
    if session.sep_output_path is None or not session.sep_output_path.exists():
        raise HTTPException(status_code=404, detail="SEP processed file not available")
    return FileResponse(
        path=str(session.sep_output_path),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        filename=f"sep_procesado_{session_id[:8]}.xlsx",
    )


@router.get("/{session_id}/download/pie")
async def download_pie(session_id: str):
    """Download the processed NORMAL/PIE file."""
    session = _get_session_or_404(session_id)
    if session.status != ProcessingStatus.COMPLETED:
        raise HTTPException(status_code=409, detail="Processing not completed")
    if session.pie_output_path is None or not session.pie_output_path.exists():
        raise HTTPException(status_code=404, detail="PIE processed file not available")
    return FileResponse(
        path=str(session.pie_output_path),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        filename=f"normal_pie_procesado_{session_id[:8]}.xlsx",
    )


@router.get("/{session_id}/download/brp")
async def download_brp(session_id: str):
    """Download the BRP distributed file (main output)."""
    session = _get_session_or_404(session_id)
    if session.status != ProcessingStatus.COMPLETED:
        raise HTTPException(status_code=409, detail="Processing not completed")
    if session.output_path is None or not session.output_path.exists():
        raise HTTPException(status_code=404, detail="BRP output file not found")
    return FileResponse(
        path=str(session.output_path),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        filename=f"brp_distribuido_{session_id[:8]}.xlsx",
    )


@router.get("/{session_id}/download/combo")
async def download_combo(session_id: str):
    """Download combined Excel with all sheets."""
    session = _get_session_or_404(session_id)
    if session.status != ProcessingStatus.COMPLETED:
        raise HTTPException(status_code=409, detail="Processing not completed")
    if session.output_path is None or not session.output_path.exists():
        raise HTTPException(status_code=404, detail="Output file not found")
    return FileResponse(
        path=str(session.output_path),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        filename=f"remupro_completo_{session_id[:8]}.xlsx",
    )


@router.get("/{session_id}/download/word")
async def download_word(session_id: str):
    """Generate and download Word report."""
    import tempfile
    import os

    session = _get_session_or_404(session_id)
    if session.status != ProcessingStatus.COMPLETED:
        raise HTTPException(status_code=409, detail="Processing not completed")
    if session.result_df is None:
        raise HTTPException(status_code=404, detail="No result data available")

    try:
        from reports.word_report import InformeWord

        fd, word_path = tempfile.mkstemp(suffix=".docx")
        os.close(fd)

        informe = InformeWord()
        informe.generar(session.result_df, word_path, resumen=session.summary)

        return FileResponse(
            path=word_path,
            media_type="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
            filename=f"informe_brp_{session_id[:8]}.docx",
        )
    except ImportError:
        raise HTTPException(status_code=501, detail="Word report generator not available")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error generating Word report: {str(e)}")


# ---------------------------------------------------------------------------
# REM-specific results endpoint
# ---------------------------------------------------------------------------

@router.get("/{session_id}/rem", response_model=REMResultsResponse)
async def get_rem_results(session_id: str):
    """
    Get REM processing results with hour summary and alerts.
    """
    session = _get_session_or_404(session_id)

    if session.process_type != "rem":
        raise HTTPException(
            status_code=400,
            detail="This session is not a REM processing session",
        )

    if session.status == ProcessingStatus.FAILED:
        return REMResultsResponse(
            session_id=session_id,
            status=session.status,
            error=session.error,
        )

    if session.status != ProcessingStatus.COMPLETED:
        return REMResultsResponse(
            session_id=session_id,
            status=session.status,
        )

    resumen_records = _df_to_records(session.rem_resumen_df, limit=5000)

    return REMResultsResponse(
        session_id=session_id,
        status=session.status,
        total_personas=session.summary.get("total_personas", 0) if session.summary else 0,
        exceden_44=session.summary.get("exceden_44", 0) if session.summary else 0,
        resumen=resumen_records,
        alertas=session.rem_alertas,
    )
