"""
Pydantic models for API request/response schemas.
"""

from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------

class ProcessingStatus(str, Enum):
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"


class AuditLevel(str, Enum):
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"


# ---------------------------------------------------------------------------
# Upload
# ---------------------------------------------------------------------------

class FileInfo(BaseModel):
    file_id: str
    original_name: str
    size_bytes: int
    uploaded_at: str


class UploadResponse(BaseModel):
    files: List[FileInfo]
    session_id: str


# ---------------------------------------------------------------------------
# Processing requests
# ---------------------------------------------------------------------------

class BRPProcessRequest(BaseModel):
    web_file_id: str = Field(..., description="File ID for web_sostenedor (MINEDUC)")
    sep_file_id: str = Field(..., description="File ID for processed SEP file")
    pie_file_id: str = Field(..., description="File ID for processed PIE file")


class IntegradoProcessRequest(BaseModel):
    sep_bruto_file_id: str = Field(..., description="File ID for raw SEP file")
    pie_bruto_file_id: str = Field(..., description="File ID for raw PIE file")
    web_file_id: str = Field(..., description="File ID for web_sostenedor (MINEDUC)")
    mes: Optional[str] = Field(None, description="Month identifier YYYY-MM for DB storage")


class REMProcessRequest(BaseModel):
    rem_file_id: str = Field(..., description="File ID for REM file")


# ---------------------------------------------------------------------------
# Processing responses
# ---------------------------------------------------------------------------

class ProcessStartResponse(BaseModel):
    session_id: str
    status: ProcessingStatus
    message: str


class ProgressUpdate(BaseModel):
    session_id: str
    status: ProcessingStatus
    progress: int = Field(ge=0, le=100)
    message: str


# ---------------------------------------------------------------------------
# Results
# ---------------------------------------------------------------------------

class GeneralSummary(BaseModel):
    total_docentes: int = 0
    total_establecimientos: int = 0
    brp_total: int = 0
    brp_sep: int = 0
    brp_pie: int = 0
    brp_normal: int = 0
    pct_sep: float = 0.0
    pct_pie: float = 0.0
    pct_normal: float = 0.0
    daem_total: int = 0
    cpeip_total: int = 0
    reconocimiento_total: int = 0
    tramo_total: int = 0
    casos_revision: int = 0


class AuditEntryResponse(BaseModel):
    timestamp: str
    nivel: str
    tipo: str
    mensaje: str
    datos: Dict[str, Any] = Field(default_factory=dict)


class AuditLogResponse(BaseModel):
    total: int
    entries: List[AuditEntryResponse]
    summary: Dict[str, Any] = Field(default_factory=dict)


class ResultsResponse(BaseModel):
    session_id: str
    status: ProcessingStatus
    process_type: str = ""
    created_at: str = ""
    completed_at: Optional[str] = None
    summary: Optional[GeneralSummary] = None
    column_alerts: List[Dict[str, Any]] = Field(default_factory=list)
    docentes_revisar: List[Dict[str, Any]] = Field(default_factory=list)
    data_preview: List[Dict[str, Any]] = Field(
        default_factory=list,
        description="First N rows of the result DataFrame"
    )
    total_rows: int = 0
    error: Optional[str] = None


class MultiEstablishmentResponse(BaseModel):
    session_id: str
    total_docentes_multi: int = 0
    entries: List[Dict[str, Any]] = Field(default_factory=list)


# ---------------------------------------------------------------------------
# REM results
# ---------------------------------------------------------------------------

class REMResultsResponse(BaseModel):
    session_id: str
    status: ProcessingStatus
    total_personas: int = 0
    exceden_44: int = 0
    resumen: List[Dict[str, Any]] = Field(default_factory=list)
    alertas: List[Dict[str, Any]] = Field(default_factory=list)
    error: Optional[str] = None


# ---------------------------------------------------------------------------
# Dashboard
# ---------------------------------------------------------------------------

class MonthsListResponse(BaseModel):
    months: List[str] = Field(default_factory=list)


class TrendDataPoint(BaseModel):
    mes: str
    brp_total: float = 0
    brp_sep: float = 0
    brp_pie: float = 0
    brp_normal: float = 0
    total_docentes: int = 0
    total_establecimientos: int = 0
    reconocimiento_total: float = 0
    tramo_total: float = 0


class TrendsResponse(BaseModel):
    trends: List[TrendDataPoint] = Field(default_factory=list)


class TeacherSearchResponse(BaseModel):
    total: int = 0
    limit: int = 50
    offset: int = 0
    docentes: List[Dict[str, Any]] = Field(default_factory=list)


class SchoolListResponse(BaseModel):
    escuelas: List[Dict[str, Any]] = Field(default_factory=list)


class ComparisonResponse(BaseModel):
    mes_anterior: str
    mes_actual: str
    resumen: Dict[str, Any] = Field(default_factory=dict)
    cambios: List[Dict[str, Any]] = Field(default_factory=list)


class MultiEstablishmentDBResponse(BaseModel):
    total: int = 0
    docentes: List[Dict[str, Any]] = Field(default_factory=list)


# ---------------------------------------------------------------------------
# Column Preferences
# ---------------------------------------------------------------------------

class ColumnPreference(BaseModel):
    columna_key: str
    estado: str = "default"


class ColumnPreferencesResponse(BaseModel):
    preferences: List[Dict[str, Any]] = Field(default_factory=list)


class BulkColumnPreferencesRequest(BaseModel):
    preferences: List[ColumnPreference]


# ---------------------------------------------------------------------------
# Anual
# ---------------------------------------------------------------------------

class AnualProcessRequest(BaseModel):
    anual_file_id: str = Field(..., description="File ID for annual consolidated file")
    anio: Optional[int] = Field(None, description="Year of the data (auto-detected if not provided)")


class AnualSummaryResponse(BaseModel):
    anio: int = 0
    fecha_proceso: Optional[str] = None
    total_docentes: int = 0
    total_establecimientos: int = 0
    total_registros: int = 0
    brp_total_anual: float = 0
    haberes_total_anual: float = 0
    liquido_total_anual: float = 0
    notas: Optional[str] = None


class AnualTrendPoint(BaseModel):
    mes: str
    brp_total: float = 0
    brp_sep: float = 0
    brp_pie: float = 0
    brp_normal: float = 0
    brp_eib: float = 0
    docentes: int = 0
    haberes_total: float = 0


class AnualTrendsResponse(BaseModel):
    anio: int
    trends: List[AnualTrendPoint] = Field(default_factory=list)


class AnualYearsResponse(BaseModel):
    years: List[int] = Field(default_factory=list)


class AnualSchoolsResponse(BaseModel):
    escuelas: List[Dict[str, Any]] = Field(default_factory=list)


class AnualMultiEstablishmentResponse(BaseModel):
    total: int = 0
    docentes: List[Dict[str, Any]] = Field(default_factory=list)


# ---------------------------------------------------------------------------
# Health
# ---------------------------------------------------------------------------

class HealthResponse(BaseModel):
    status: str = "ok"
    version: str = "3.0.0"
    timestamp: str
    processors_available: List[str] = Field(default_factory=list)
