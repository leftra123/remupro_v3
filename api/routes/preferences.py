"""
Column alert preferences endpoints.

Allows users to configure how column alerts are handled:
- default: normal behavior
- ignore: suppress alerts for this column
- important: highlight alerts for this column
"""

import logging

from fastapi import APIRouter, HTTPException

from api.models import (
    BulkColumnPreferencesRequest,
    ColumnPreference,
    ColumnPreferencesResponse,
)

logger = logging.getLogger("api.preferences")

router = APIRouter(prefix="/api/preferences", tags=["preferences"])


def _get_repo():
    from database.repository import BRPRepository
    return BRPRepository()


@router.get("/columns", response_model=ColumnPreferencesResponse)
async def get_column_preferences():
    """Obtener todas las preferencias de columnas."""
    repo = _get_repo()
    prefs = repo.obtener_preferencias_columnas()
    return ColumnPreferencesResponse(preferences=prefs)


@router.put("/columns/{key}")
async def update_column_preference(key: str, pref: ColumnPreference):
    """Actualizar preferencia de una columna."""
    repo = _get_repo()
    try:
        result = repo.guardar_preferencia_columna(key, pref.estado)
        return {
            "columna_key": result.columna_key,
            "estado": result.estado,
            "updated_at": result.updated_at.isoformat() if result.updated_at else None,
        }
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.delete("/columns/{key}")
async def delete_column_preference(key: str):
    """Eliminar preferencia (reset a default)."""
    repo = _get_repo()
    deleted = repo.eliminar_preferencia_columna(key)
    if not deleted:
        raise HTTPException(status_code=404, detail=f"Preferencia no encontrada: {key}")
    return {"deleted": True, "columna_key": key}


@router.post("/columns/bulk", response_model=ColumnPreferencesResponse)
async def bulk_update_preferences(request: BulkColumnPreferencesRequest):
    """Actualizar multiples preferencias a la vez."""
    repo = _get_repo()
    for pref in request.preferences:
        try:
            repo.guardar_preferencia_columna(pref.columna_key, pref.estado)
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))
    prefs = repo.obtener_preferencias_columnas()
    return ColumnPreferencesResponse(preferences=prefs)
