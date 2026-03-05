"""Tests for the RVTools parser — chunk_and_write function."""
import json
from pathlib import Path

import pandas as pd
import pytest

from src.rvtools_parser import chunk_and_write


@pytest.fixture
def sample_xlsx(tmp_path: Path) -> Path:
    """Create a minimal XLSX file for testing."""
    import openpyxl
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "vInfo"
    headers = ["Name", "Host", "CPUs", "Memory", "Powerstate"]
    ws.append(headers)
    for i in range(25):
        ws.append([f"vm-{i:03d}", f"host-{i % 3}", 4, 8192, "poweredOn"])
    path = tmp_path / "test_rvtools.xlsx"
    wb.save(str(path))
    return path


def test_chunk_and_write_creates_chunks(sample_xlsx, tmp_path):
    out_dir = tmp_path / "chunks"
    result = chunk_and_write(str(sample_xlsx), "vInfo", str(out_dir), chunk_size=10)

    assert result["manifest_path"]
    manifest = result["manifest"]
    assert manifest["total_rows"] == 25
    assert manifest["chunk_count"] == 3  # 10 + 10 + 5

    # Verify parquet files exist
    for ch in manifest["chunks"]:
        p = Path(ch["local_path"])
        assert p.exists()
        df = pd.read_parquet(p)
        assert len(df) == ch["rows"]


def test_chunk_uses_sha256(sample_xlsx, tmp_path):
    """Verify that chunks use SHA-256 instead of MD5."""
    out_dir = tmp_path / "chunks"
    result = chunk_and_write(str(sample_xlsx), "vInfo", str(out_dir), chunk_size=100)

    for ch in result["manifest"]["chunks"]:
        assert "sha256" in ch
        assert len(ch["sha256"]) == 64  # SHA-256 hex digest length
        assert "md5" not in ch


def test_chunk_with_missing_sheet(sample_xlsx, tmp_path):
    out_dir = tmp_path / "chunks"
    with pytest.raises(ValueError, match="not found"):
        chunk_and_write(str(sample_xlsx), "NonExistentSheet", str(out_dir))


def test_chunk_with_missing_file(tmp_path):
    out_dir = tmp_path / "chunks"
    with pytest.raises(FileNotFoundError):
        chunk_and_write(str(tmp_path / "missing.xlsx"), "vInfo", str(out_dir))


def test_manifest_json_is_valid(sample_xlsx, tmp_path):
    out_dir = tmp_path / "chunks"
    result = chunk_and_write(str(sample_xlsx), "vInfo", str(out_dir), chunk_size=100, ingest_id="test123")

    mp = Path(result["manifest_path"])
    assert mp.exists()
    m = json.loads(mp.read_text(encoding="utf-8"))
    assert m["ingest_id"] == "test123"
    assert m["sheet"] == "vInfo"
    assert "generated_at_utc" in m
