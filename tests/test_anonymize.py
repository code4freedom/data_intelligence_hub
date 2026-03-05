"""Tests for the anonymization module."""
import json
from pathlib import Path

import pandas as pd

from src.backend.anonymize import anonymize_manifest_chunks, _is_sensitive_column


def test_sensitive_column_detection():
    assert _is_sensitive_column("VM Name") is True
    assert _is_sensitive_column("Host") is True
    assert _is_sensitive_column("Cluster") is True
    assert _is_sensitive_column("Datastore") is True
    assert _is_sensitive_column("IP Address") is True

    # These should NOT be sensitive
    assert _is_sensitive_column("CPUs") is False
    assert _is_sensitive_column("Memory") is False
    assert _is_sensitive_column("Powerstate") is False
    assert _is_sensitive_column("OS according to the configuration file") is False


def test_anonymize_masks_sensitive_columns(tmp_path: Path):
    rows = [
        {"VM": "prod-sql-01", "Host": "esxi-host-01", "CPUs": 4, "Memory": 8192, "Powerstate": "poweredOn"},
        {"VM": "prod-web-02", "Host": "esxi-host-02", "CPUs": 2, "Memory": 4096, "Powerstate": "poweredOff"},
    ]
    chunk_path = tmp_path / "chunk_vInfo_000000.parquet"
    pd.DataFrame(rows).to_parquet(chunk_path, index=False)

    manifest = {
        "ingest_id": "test",
        "sheet": "vInfo",
        "chunk_count": 1,
        "total_rows": 2,
        "chunks": [{"name": chunk_path.name, "rows": 2, "local_path": str(chunk_path)}],
    }

    result = anonymize_manifest_chunks(manifest, seed="test-seed")

    assert result["anonymized"] is True
    assert len(result["anonymized_columns"]) > 0

    # Verify underlying data was masked
    df = pd.read_parquet(chunk_path)
    for val in df["VM"].tolist():
        assert "prod-sql" not in str(val)
        assert "prod-web" not in str(val)

    # Numeric columns should NOT be modified
    assert df["CPUs"].tolist() == [4, 2]
    assert df["Memory"].tolist() == [8192, 4096]


def test_anonymize_is_deterministic(tmp_path: Path):
    """Same seed + same data should produce identical masks."""
    rows = [{"VM": "test-vm-01", "Host": "host-01", "CPUs": 4, "Memory": 8192}]

    results = []
    for i in range(2):
        chunk_path = tmp_path / f"chunk_{i}.parquet"
        pd.DataFrame(rows).to_parquet(chunk_path, index=False)
        manifest = {
            "ingest_id": "det_test",
            "sheet": "vInfo",
            "chunk_count": 1,
            "total_rows": 1,
            "chunks": [{"name": chunk_path.name, "rows": 1, "local_path": str(chunk_path)}],
        }
        anonymize_manifest_chunks(manifest, seed="deterministic-seed")
        df = pd.read_parquet(chunk_path)
        results.append(df["VM"].iloc[0])

    assert results[0] == results[1]
