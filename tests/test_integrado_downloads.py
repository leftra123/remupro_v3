"""
Tests for individual download endpoints and intermediate file preservation.

Uses TestClient with mocked session store to simulate completed processing.
"""

import tempfile
from pathlib import Path
from unittest.mock import patch, MagicMock
from datetime import datetime

import pytest
from fastapi.testclient import TestClient

from api.main import app
from api.models import ProcessingStatus

client = TestClient(app)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def temp_excel():
    """Create a temporary file simulating an Excel output."""
    import os
    fd, path = tempfile.mkstemp(suffix=".xlsx")
    os.write(fd, b"PK\x03\x04fake-xlsx-content")  # Minimal ZIP header
    os.close(fd)
    yield Path(path)
    if os.path.exists(path):
        os.unlink(path)


@pytest.fixture
def completed_session(temp_excel):
    """Create a mock completed session with all output paths."""
    session = MagicMock()
    session.session_id = "test-session-123"
    session.status = ProcessingStatus.COMPLETED
    session.process_type = "integrado"
    session.created_at = datetime(2026, 1, 15, 10, 0, 0)
    session.completed_at = datetime(2026, 1, 15, 10, 5, 0)
    session.output_path = temp_excel
    session.sep_output_path = temp_excel
    session.pie_output_path = temp_excel
    session.result_df = MagicMock()
    session.result_df.__len__ = MagicMock(return_value=50)
    session.summary = {
        "total_docentes": 50,
        "total_establecimientos": 5,
        "brp_total": 10000000,
    }
    session.error = None
    session.column_alerts = []
    session.docentes_revisar = []
    session.multi_establishment_df = None
    session.audit_entries = []
    session.rem_resumen_df = None
    session.rem_alertas = []
    session.mes = "2026-01"
    return session


@pytest.fixture
def incomplete_session():
    """Create a mock session that hasn't completed processing."""
    session = MagicMock()
    session.session_id = "test-session-pending"
    session.status = ProcessingStatus.PROCESSING
    session.process_type = "integrado"
    return session


STORE_PATCH = "api.routes.data.store"


# ---------------------------------------------------------------------------
# Download endpoint tests
# ---------------------------------------------------------------------------

class TestDownloadSEP:
    @patch(STORE_PATCH)
    def test_download_sep_success(self, mock_store, completed_session):
        mock_store.get_session.return_value = completed_session
        resp = client.get("/api/results/test-session-123/download/sep")
        assert resp.status_code == 200
        assert "sep_procesado_" in resp.headers.get("content-disposition", "")

    @patch(STORE_PATCH)
    def test_download_sep_no_file(self, mock_store, completed_session):
        completed_session.sep_output_path = None
        mock_store.get_session.return_value = completed_session
        resp = client.get("/api/results/test-session-123/download/sep")
        assert resp.status_code == 404

    @patch(STORE_PATCH)
    def test_download_sep_not_completed(self, mock_store, incomplete_session):
        mock_store.get_session.return_value = incomplete_session
        resp = client.get("/api/results/test-session-pending/download/sep")
        assert resp.status_code == 409


class TestDownloadPIE:
    @patch(STORE_PATCH)
    def test_download_pie_success(self, mock_store, completed_session):
        mock_store.get_session.return_value = completed_session
        resp = client.get("/api/results/test-session-123/download/pie")
        assert resp.status_code == 200
        assert "normal_pie_procesado_" in resp.headers.get("content-disposition", "")

    @patch(STORE_PATCH)
    def test_download_pie_no_file(self, mock_store, completed_session):
        completed_session.pie_output_path = None
        mock_store.get_session.return_value = completed_session
        resp = client.get("/api/results/test-session-123/download/pie")
        assert resp.status_code == 404


class TestDownloadBRP:
    @patch(STORE_PATCH)
    def test_download_brp_success(self, mock_store, completed_session):
        mock_store.get_session.return_value = completed_session
        resp = client.get("/api/results/test-session-123/download/brp")
        assert resp.status_code == 200
        assert "brp_distribuido_" in resp.headers.get("content-disposition", "")

    @patch(STORE_PATCH)
    def test_download_brp_no_file(self, mock_store, completed_session):
        completed_session.output_path = None
        mock_store.get_session.return_value = completed_session
        resp = client.get("/api/results/test-session-123/download/brp")
        assert resp.status_code == 404


class TestDownloadCombo:
    @patch(STORE_PATCH)
    def test_download_combo_success(self, mock_store, completed_session):
        mock_store.get_session.return_value = completed_session
        resp = client.get("/api/results/test-session-123/download/combo")
        assert resp.status_code == 200
        assert "remupro_completo_" in resp.headers.get("content-disposition", "")


class TestDownloadWord:
    @patch(STORE_PATCH)
    def test_download_word_no_data(self, mock_store, completed_session):
        completed_session.result_df = None
        mock_store.get_session.return_value = completed_session
        resp = client.get("/api/results/test-session-123/download/word")
        assert resp.status_code == 404


class TestSessionNotFound:
    @patch(STORE_PATCH)
    def test_session_not_found(self, mock_store):
        mock_store.get_session.return_value = None
        resp = client.get("/api/results/nonexistent/download/sep")
        assert resp.status_code == 404

    @patch(STORE_PATCH)
    def test_main_results_not_found(self, mock_store):
        mock_store.get_session.return_value = None
        resp = client.get("/api/results/nonexistent")
        assert resp.status_code == 404


# ---------------------------------------------------------------------------
# Main Excel download
# ---------------------------------------------------------------------------

class TestDownloadExcel:
    @patch(STORE_PATCH)
    def test_download_excel_success(self, mock_store, completed_session):
        mock_store.get_session.return_value = completed_session
        resp = client.get("/api/results/test-session-123/download/excel")
        assert resp.status_code == 200
