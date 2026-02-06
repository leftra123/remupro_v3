"""
In-memory session store for tracking uploaded files and processing results.

Each session holds references to uploaded temp files, processing status,
and result DataFrames. Sessions are identified by UUID strings.
"""

import os
import tempfile
import threading
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd

from api.models import ProcessingStatus


class SessionData:
    """Holds all data associated with a processing session."""

    def __init__(self, session_id: str):
        self.session_id: str = session_id
        self.created_at: datetime = datetime.now()
        self.completed_at: Optional[datetime] = None
        self.status: ProcessingStatus = ProcessingStatus.PENDING
        self.process_type: str = ""
        self.progress: int = 0
        self.progress_message: str = ""
        self.error: Optional[str] = None

        # Uploaded file paths keyed by file_id
        self.files: Dict[str, Dict[str, Any]] = {}

        # Processing results
        self.result_df: Optional[pd.DataFrame] = None
        self.multi_establishment_df: Optional[pd.DataFrame] = None
        self.summary: Optional[Dict[str, Any]] = None
        self.column_alerts: List[Dict[str, Any]] = []
        self.docentes_revisar: List[Dict[str, Any]] = []
        self.audit_entries: List[Dict[str, Any]] = []
        self.output_path: Optional[Path] = None

        # Intermediate output paths (SEP/PIE processed files)
        self.sep_output_path: Optional[Path] = None
        self.pie_output_path: Optional[Path] = None

        # Month identifier for DB storage
        self.mes: Optional[str] = None

        # REM-specific results
        self.rem_resumen_df: Optional[pd.DataFrame] = None
        self.rem_alertas: List[Dict[str, Any]] = []

        # Anual-specific results
        self.anual_mensual_df: Optional[pd.DataFrame] = None
        self.anual_resumen_df: Optional[pd.DataFrame] = None
        self.anual_escuelas_df: Optional[pd.DataFrame] = None
        self.anual_alertas: List[Dict[str, Any]] = []

    def set_completed(self) -> None:
        self.status = ProcessingStatus.COMPLETED
        self.completed_at = datetime.now()
        self.progress = 100

    def set_failed(self, error: str) -> None:
        self.status = ProcessingStatus.FAILED
        self.error = error
        self.completed_at = datetime.now()

    def cleanup_files(self) -> None:
        """Remove temporary uploaded files."""
        for file_info in self.files.values():
            path = file_info.get("temp_path")
            if path and os.path.exists(path):
                try:
                    os.unlink(path)
                except OSError:
                    pass
        if self.output_path and self.output_path.exists():
            try:
                os.unlink(self.output_path)
            except OSError:
                pass
        for intermediate in [self.sep_output_path, self.pie_output_path]:
            if intermediate and intermediate.exists():
                try:
                    os.unlink(intermediate)
                except OSError:
                    pass


class SessionStore:
    """
    Thread-safe in-memory session store.

    Provides methods to create sessions, register uploaded files,
    and retrieve processing results. Expired sessions are cleaned up
    automatically to prevent memory exhaustion and temp file leaks.
    """

    # Sessions expire after 2 hours of inactivity
    SESSION_TTL_SECONDS = 2 * 60 * 60
    # Maximum number of concurrent sessions
    MAX_SESSIONS = 100

    def __init__(self):
        self._sessions: Dict[str, SessionData] = {}
        self._lock = threading.Lock()
        # Shared temp directory for uploads
        self._upload_dir = Path(tempfile.mkdtemp(prefix="remupro_uploads_"))

    @property
    def upload_dir(self) -> Path:
        return self._upload_dir

    def _cleanup_expired_sessions(self) -> None:
        """Remove expired sessions and their temp files.

        Must be called while holding self._lock.
        Cleans up sessions that have exceeded SESSION_TTL_SECONDS since creation,
        preventing unbounded memory growth and temp file accumulation.
        """
        now = datetime.now()
        expired_ids = []
        for sid, session in self._sessions.items():
            age = (now - session.created_at).total_seconds()
            if age > self.SESSION_TTL_SECONDS:
                expired_ids.append(sid)

        for sid in expired_ids:
            session = self._sessions.pop(sid, None)
            if session:
                session.cleanup_files()

    def create_session(self) -> SessionData:
        session_id = str(uuid.uuid4())
        session = SessionData(session_id)
        with self._lock:
            # Clean up expired sessions before creating new ones
            self._cleanup_expired_sessions()
            # Enforce maximum session limit to prevent resource exhaustion
            if len(self._sessions) >= self.MAX_SESSIONS:
                # Remove oldest session
                oldest_id = min(
                    self._sessions,
                    key=lambda sid: self._sessions[sid].created_at
                )
                old_session = self._sessions.pop(oldest_id, None)
                if old_session:
                    old_session.cleanup_files()
            self._sessions[session_id] = session
        return session

    def get_session(self, session_id: str) -> Optional[SessionData]:
        with self._lock:
            return self._sessions.get(session_id)

    def get_or_create_session(self, session_id: Optional[str] = None) -> SessionData:
        if session_id:
            session = self.get_session(session_id)
            if session:
                return session
        return self.create_session()

    def register_file(
        self,
        session_id: str,
        original_name: str,
        temp_path: str,
        size_bytes: int,
    ) -> str:
        """Register an uploaded file and return its file_id."""
        file_id = str(uuid.uuid4())
        session = self.get_session(session_id)
        if not session:
            raise ValueError(f"Session not found: {session_id}")

        session.files[file_id] = {
            "file_id": file_id,
            "original_name": original_name,
            "temp_path": temp_path,
            "size_bytes": size_bytes,
            "uploaded_at": datetime.now().isoformat(),
        }
        return file_id

    def get_file_path(self, session_id: str, file_id: str) -> Optional[Path]:
        """Resolve a file_id to its temporary path on disk."""
        session = self.get_session(session_id)
        if not session:
            return None
        file_info = session.files.get(file_id)
        if not file_info:
            # Search across all sessions (file_id is globally unique)
            for s in self._sessions.values():
                if file_id in s.files:
                    file_info = s.files[file_id]
                    break
        if not file_info:
            return None
        temp_path = file_info.get("temp_path")
        if temp_path and os.path.exists(temp_path):
            return Path(temp_path)
        return None

    def resolve_file(self, file_id: str) -> Optional[Path]:
        """Resolve a file_id to path, searching all sessions."""
        with self._lock:
            for session in self._sessions.values():
                if file_id in session.files:
                    temp_path = session.files[file_id].get("temp_path")
                    if temp_path and os.path.exists(temp_path):
                        return Path(temp_path)
        return None

    def delete_session(self, session_id: str) -> bool:
        with self._lock:
            session = self._sessions.pop(session_id, None)
            if session:
                session.cleanup_files()
                return True
            return False

    def list_sessions(self) -> List[Dict[str, Any]]:
        with self._lock:
            return [
                {
                    "session_id": s.session_id,
                    "status": s.status.value,
                    "process_type": s.process_type,
                    "created_at": s.created_at.isoformat(),
                    "progress": s.progress,
                }
                for s in self._sessions.values()
            ]


# Global singleton
store = SessionStore()
