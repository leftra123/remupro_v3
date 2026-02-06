"""
Tests for dashboard and preferences API endpoints.

Uses TestClient with mocked BRPRepository to avoid DB dependencies.
"""

import pytest
from unittest.mock import patch, MagicMock
from fastapi.testclient import TestClient

from api.main import app

client = TestClient(app)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _mock_repo():
    """Create a mock BRPRepository."""
    repo = MagicMock()
    repo.obtener_meses_disponibles.return_value = ["2026-01", "2026-02"]
    repo.obtener_resumen_mes.return_value = {
        "mes": "2026-01",
        "total_docentes": 50,
        "total_establecimientos": 5,
        "brp_total": 10000000,
        "brp_sep": 4000000,
        "brp_pie": 3000000,
        "brp_normal": 3000000,
    }
    repo.obtener_tendencias.return_value = [
        {
            "mes": "2026-01",
            "brp_total": 10000000,
            "brp_sep": 4000000,
            "brp_pie": 3000000,
            "brp_normal": 3000000,
            "total_docentes": 50,
            "total_establecimientos": 5,
            "reconocimiento_total": 0,
            "tramo_total": 0,
        }
    ]
    repo.buscar_docentes.return_value = {
        "total": 2,
        "limit": 50,
        "offset": 0,
        "docentes": [
            {"rut": "12345678-9", "nombre": "Juan Perez", "rbd": "1001"},
            {"rut": "98765432-1", "nombre": "Maria Lopez", "rbd": "1002"},
        ],
    }
    repo.obtener_escuelas.return_value = [
        {"rbd": "1001", "nombre": "Escuela A", "docentes": 25, "brp_total": 5000000},
        {"rbd": "1002", "nombre": "Escuela B", "docentes": 25, "brp_total": 5000000},
    ]
    repo.obtener_docentes_multi_establecimiento.return_value = [
        {"rut": "12345678-9", "nombre": "Juan Perez", "rbds": ["1001", "1002"]},
    ]
    repo.obtener_preferencias_columnas.return_value = [
        {"columna_key": "BRP_TOTAL", "estado": "default", "updated_at": None},
    ]
    pref_obj = MagicMock()
    pref_obj.columna_key = "BRP_TOTAL"
    pref_obj.estado = "important"
    pref_obj.updated_at = None
    repo.guardar_preferencia_columna.return_value = pref_obj
    repo.eliminar_preferencia_columna.return_value = True
    return repo


REPO_PATCH = "api.routes.dashboard._get_repo"
PREF_REPO_PATCH = "api.routes.preferences._get_repo"


# ---------------------------------------------------------------------------
# Dashboard endpoint tests
# ---------------------------------------------------------------------------

class TestDashboardMonths:
    @patch(REPO_PATCH)
    def test_get_months(self, mock_get_repo):
        mock_get_repo.return_value = _mock_repo()
        resp = client.get("/api/dashboard/months")
        assert resp.status_code == 200
        data = resp.json()
        assert "months" in data
        assert len(data["months"]) == 2
        assert "2026-01" in data["months"]

    @patch(REPO_PATCH)
    def test_get_months_empty(self, mock_get_repo):
        repo = _mock_repo()
        repo.obtener_meses_disponibles.return_value = []
        mock_get_repo.return_value = repo
        resp = client.get("/api/dashboard/months")
        assert resp.status_code == 200
        assert resp.json()["months"] == []


class TestDashboardSummary:
    @patch(REPO_PATCH)
    def test_get_summary(self, mock_get_repo):
        mock_get_repo.return_value = _mock_repo()
        resp = client.get("/api/dashboard/summary/2026-01")
        assert resp.status_code == 200
        data = resp.json()
        assert data["total_docentes"] == 50

    @patch(REPO_PATCH)
    def test_get_summary_not_found(self, mock_get_repo):
        repo = _mock_repo()
        repo.obtener_resumen_mes.return_value = None
        mock_get_repo.return_value = repo
        resp = client.get("/api/dashboard/summary/2099-01")
        assert resp.status_code == 404


class TestDashboardTrends:
    @patch(REPO_PATCH)
    def test_get_trends(self, mock_get_repo):
        mock_get_repo.return_value = _mock_repo()
        resp = client.get("/api/dashboard/trends")
        assert resp.status_code == 200
        data = resp.json()
        assert "trends" in data
        assert len(data["trends"]) >= 1
        assert data["trends"][0]["mes"] == "2026-01"


class TestDashboardTeachers:
    @patch(REPO_PATCH)
    def test_search_teachers(self, mock_get_repo):
        mock_get_repo.return_value = _mock_repo()
        resp = client.get("/api/dashboard/teachers/2026-01?q=Juan")
        assert resp.status_code == 200
        data = resp.json()
        assert data["total"] == 2
        assert len(data["docentes"]) == 2

    @patch(REPO_PATCH)
    def test_search_with_rbd_filter(self, mock_get_repo):
        mock_get_repo.return_value = _mock_repo()
        resp = client.get("/api/dashboard/teachers/2026-01?rbd=1001")
        assert resp.status_code == 200

    @patch(REPO_PATCH)
    def test_pagination_params(self, mock_get_repo):
        mock_get_repo.return_value = _mock_repo()
        resp = client.get("/api/dashboard/teachers/2026-01?limit=10&offset=5")
        assert resp.status_code == 200


class TestDashboardSchools:
    @patch(REPO_PATCH)
    def test_get_schools(self, mock_get_repo):
        mock_get_repo.return_value = _mock_repo()
        resp = client.get("/api/dashboard/schools/2026-01")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data["escuelas"]) == 2


class TestDashboardMultiEstablishment:
    @patch(REPO_PATCH)
    def test_get_multi_establishment(self, mock_get_repo):
        mock_get_repo.return_value = _mock_repo()
        resp = client.get("/api/dashboard/multi-establishment/2026-01")
        assert resp.status_code == 200
        data = resp.json()
        assert data["total"] == 1


# ---------------------------------------------------------------------------
# Preferences endpoint tests
# ---------------------------------------------------------------------------

class TestColumnPreferences:
    @patch(PREF_REPO_PATCH)
    def test_get_preferences(self, mock_get_repo):
        mock_get_repo.return_value = _mock_repo()
        resp = client.get("/api/preferences/columns")
        assert resp.status_code == 200
        data = resp.json()
        assert "preferences" in data

    @patch(PREF_REPO_PATCH)
    def test_update_preference(self, mock_get_repo):
        mock_get_repo.return_value = _mock_repo()
        resp = client.put(
            "/api/preferences/columns/BRP_TOTAL",
            json={"columna_key": "BRP_TOTAL", "estado": "important"},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["estado"] == "important"

    @patch(PREF_REPO_PATCH)
    def test_delete_preference(self, mock_get_repo):
        mock_get_repo.return_value = _mock_repo()
        resp = client.delete("/api/preferences/columns/BRP_TOTAL")
        assert resp.status_code == 200
        assert resp.json()["deleted"] is True

    @patch(PREF_REPO_PATCH)
    def test_delete_preference_not_found(self, mock_get_repo):
        repo = _mock_repo()
        repo.eliminar_preferencia_columna.return_value = False
        mock_get_repo.return_value = repo
        resp = client.delete("/api/preferences/columns/NONEXISTENT")
        assert resp.status_code == 404

    @patch(PREF_REPO_PATCH)
    def test_bulk_update(self, mock_get_repo):
        mock_get_repo.return_value = _mock_repo()
        resp = client.post(
            "/api/preferences/columns/bulk",
            json={
                "preferences": [
                    {"columna_key": "BRP_TOTAL", "estado": "important"},
                    {"columna_key": "HORAS_CONTRATO", "estado": "ignore"},
                ]
            },
        )
        assert resp.status_code == 200
        assert "preferences" in resp.json()


# ---------------------------------------------------------------------------
# Health check
# ---------------------------------------------------------------------------

class TestHealth:
    def test_health_check(self):
        resp = client.get("/api/health")
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "ok"
        assert data["version"] == "3.0.0"
