"""
Dashboard endpoints.

Provides access to historical data, trends, teacher search,
and school breakdowns from the database.
"""

import logging
from typing import Optional

from fastapi import APIRouter, HTTPException, Query

from api.models import (
    ComparisonResponse,
    MonthsListResponse,
    MultiEstablishmentDBResponse,
    SchoolListResponse,
    TeacherSearchResponse,
    TrendDataPoint,
    TrendsResponse,
)

logger = logging.getLogger("api.dashboard")

router = APIRouter(prefix="/api/dashboard", tags=["dashboard"])


def _get_repo():
    from database.repository import BRPRepository
    return BRPRepository()


@router.get("/months", response_model=MonthsListResponse)
async def get_months():
    """Lista de meses con procesamiento disponible."""
    repo = _get_repo()
    months = repo.obtener_meses_disponibles()
    return MonthsListResponse(months=months)


@router.get("/summary/{mes}")
async def get_month_summary(mes: str):
    """Resumen de un mes especifico."""
    repo = _get_repo()
    resumen = repo.obtener_resumen_mes(mes)
    if not resumen:
        raise HTTPException(status_code=404, detail=f"No hay datos para el mes {mes}")
    return resumen


@router.get("/trends", response_model=TrendsResponse)
async def get_trends():
    """Tendencias multi-mes para grafico de area."""
    repo = _get_repo()
    data = repo.obtener_tendencias()
    trends = [TrendDataPoint(**d) for d in data]
    return TrendsResponse(trends=trends)


@router.get("/comparison", response_model=ComparisonResponse)
async def get_comparison(
    mes_anterior: str = Query(..., description="Mes anterior (YYYY-MM)"),
    mes_actual: str = Query(..., description="Mes actual (YYYY-MM)"),
):
    """Comparacion entre dos meses."""
    try:
        from database.comparador import ComparadorMeses
        repo = _get_repo()
        comparador = ComparadorMeses(repo)
        resultado = comparador.comparar(mes_anterior, mes_actual)
        return ComparisonResponse(
            mes_anterior=mes_anterior,
            mes_actual=mes_actual,
            resumen=resultado.get("resumen", {}),
            cambios=resultado.get("cambios", []),
        )
    except Exception as e:
        raise HTTPException(status_code=400, detail="Error al comparar meses. Verifique que ambos meses existan.")


@router.get("/teachers/{mes}", response_model=TeacherSearchResponse)
async def search_teachers(
    mes: str,
    q: str = Query("", description="Buscar por RUT o nombre"),
    rbd: str = Query("", description="Filtrar por RBD"),
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
):
    """Busqueda paginada de docentes en un mes."""
    repo = _get_repo()
    result = repo.buscar_docentes(mes, query=q, rbd=rbd, limit=limit, offset=offset)
    return TeacherSearchResponse(**result)


@router.get("/schools/{mes}", response_model=SchoolListResponse)
async def get_schools(mes: str):
    """Listado de escuelas con conteo de docentes y BRP total."""
    repo = _get_repo()
    escuelas = repo.obtener_escuelas(mes)
    return SchoolListResponse(escuelas=escuelas)


@router.get("/multi-establishment/{mes}", response_model=MultiEstablishmentDBResponse)
async def get_multi_establishment_db(mes: str):
    """Docentes en 2+ establecimientos desde la base de datos."""
    repo = _get_repo()
    docentes = repo.obtener_docentes_multi_establecimiento(mes)
    return MultiEstablishmentDBResponse(
        total=len(docentes),
        docentes=docentes,
    )
