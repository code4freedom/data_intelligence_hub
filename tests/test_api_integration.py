"""Integration tests for the FastAPI backend using TestClient.

These tests exercise the API endpoints without requiring Docker services
(Redis, PostgreSQL, Neo4j) by mocking external dependencies where needed.
"""
import json
import io
from pathlib import Path
from unittest.mock import patch, MagicMock

import pandas as pd
import pytest
from fastapi.testclient import TestClient


@pytest.fixture(autouse=True)
def _mock_redis():
    """Prevent real Redis connections during import of app module."""
    mock_redis = MagicMock()
    mock_queue = MagicMock()
    with patch("redis.from_url", return_value=mock_redis), \
         patch("rq.Queue", return_value=mock_queue):
        yield mock_redis, mock_queue


@pytest.fixture
def client(_mock_redis):
    """Create a TestClient backed by a temporary data directory."""
    with patch("src.backend.app.DATA_DIR", Path("/tmp/test_vcf_data")), \
         patch("src.backend.app.RAW_DIR", Path("/tmp/test_vcf_data/raw")), \
         patch("src.backend.app.CHUNKS_DIR", Path("/tmp/test_vcf_data/chunks")), \
         patch("src.backend.app.MANIFESTS_DIR", Path("/tmp/test_vcf_data/manifests")):
        Path("/tmp/test_vcf_data/raw").mkdir(parents=True, exist_ok=True)
        Path("/tmp/test_vcf_data/chunks").mkdir(parents=True, exist_ok=True)
        Path("/tmp/test_vcf_data/manifests").mkdir(parents=True, exist_ok=True)
        from src.backend.app import app
        yield TestClient(app)


class TestHealthEndpoints:
    def test_health(self, client):
        resp = client.get("/health")
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "ok"

    def test_ready(self, client):
        resp = client.get("/ready")
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "ready"


class TestUploadValidation:
    def test_upload_rejects_non_xlsx(self, client):
        """Files that aren't .xlsx or .xls should be rejected."""
        resp = client.post(
            "/upload",
            files={"file": ("malicious.exe", b"MZ...", "application/octet-stream")},
            data={"project": "default"},
        )
        assert resp.status_code == 400
        assert "Unsupported file type" in resp.json()["error"]

    def test_upload_rejects_oversized(self, client):
        """Files exceeding MAX_UPLOAD_SIZE_MB should be rejected."""
        with patch("src.backend.app.MAX_UPLOAD_SIZE_BYTES", 100):
            from src.backend.app import app
            test_client = TestClient(app)
            resp = test_client.post(
                "/upload",
                files={"file": ("test.xlsx", b"x" * 200, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")},
                data={"project": "default"},
            )
            assert resp.status_code == 413
            assert "too large" in resp.json()["error"]

    def test_upload_sanitizes_filename(self, client):
        """Filenames with path traversal attempts should be sanitized."""
        resp = client.post(
            "/upload",
            files={"file": ("../../etc/passwd.xlsx", b"PK\x03\x04", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")},
            data={"project": "default"},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert "/" not in data["filename"]
        assert "\\" not in data["filename"]
        assert ".." not in data["filename"]

    def test_upload_accepts_xlsx(self, client):
        """Valid .xlsx files should be accepted."""
        resp = client.post(
            "/upload",
            files={"file": ("RVTools.xlsx", b"PK\x03\x04", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")},
            data={"project": "default"},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["filename"] == "RVTools.xlsx"


class TestProjectEndpoints:
    def test_list_projects(self, client):
        resp = client.get("/projects")
        assert resp.status_code == 200
        assert "projects" in resp.json()

    def test_create_project(self, client):
        resp = client.post(
            "/projects/create",
            data={"name": "Test Project", "anonymize_default": "false"},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["project"] == "test-project"

    def test_delete_nonexistent_project(self, client):
        resp = client.delete("/projects/nonexistent-project-xyz")
        assert resp.status_code == 404


class TestManifestEndpoints:
    def test_list_manifests_empty(self, client):
        resp = client.get("/manifests?project=default")
        assert resp.status_code == 200
        data = resp.json()
        assert "manifests" in data

    def test_get_nonexistent_manifest(self, client):
        resp = client.get("/manifests/does_not_exist.json?project=default")
        assert resp.status_code == 404


class TestKpiEndpoints:
    def test_kpis_empty(self, client):
        resp = client.get("/kpis?project=default")
        assert resp.status_code == 200
        data = resp.json()
        assert data["total_vms"] == 0
        assert "total_memory_tb" in data

    def test_enterprise_kpis_no_manifests(self, client):
        resp = client.get("/kpis/enterprise?project=default")
        assert resp.status_code == 404


class TestSecureFilename:
    def test_secure_filename_strips_path(self):
        from src.backend.app import _secure_filename
        assert _secure_filename("../../etc/passwd.xlsx") == "passwd.xlsx"

    def test_secure_filename_replaces_unsafe_chars(self):
        from src.backend.app import _secure_filename
        result = _secure_filename("my file (1).xlsx")
        assert " " not in result
        assert "(" not in result

    def test_secure_filename_strips_dots(self):
        from src.backend.app import _secure_filename
        result = _secure_filename("..hidden.xlsx")
        assert not result.startswith(".")
