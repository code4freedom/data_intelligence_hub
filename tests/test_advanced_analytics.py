import json
from pathlib import Path

import pandas as pd

from src.backend.advanced_analytics import compute_advanced_analytics


def _write_manifest(base: Path, ingest_id: str, rows: list[dict], ts: str):
    chunks = base / "chunks"
    manifests = base / "manifests"
    chunks.mkdir(parents=True, exist_ok=True)
    manifests.mkdir(parents=True, exist_ok=True)
    chunk_name = f"chunk_vInfo_{ingest_id}.parquet"
    chunk_path = chunks / chunk_name
    pd.DataFrame(rows).to_parquet(chunk_path, index=False)
    m = {
        "ingest_id": ingest_id,
        "sheet": "vInfo",
        "generated_at_utc": ts,
        "chunk_count": 1,
        "total_rows": len(rows),
        "chunks": [{"name": chunk_name, "rows": len(rows), "local_path": str(chunk_path)}],
    }
    mp = manifests / f"manifest_vInfo_{ingest_id}.json"
    mp.write_text(json.dumps(m), encoding="utf-8")
    return mp, manifests, chunks


def test_advanced_analytics_outputs(tmp_path: Path):
    rows_a = [
        {"VM": "sql-01", "Powerstate": "poweredOn", "CPUs": 8, "Memory": 16384, "Provisioned MB": 80000, "In Use MB": 50000, "Cluster": "A", "Host": "h1", "OS according to the configuration file": "Windows 2012"},
        {"VM": "web-01", "Powerstate": "poweredOn", "CPUs": 4, "Memory": 8192, "Provisioned MB": 40000, "In Use MB": 25000, "Cluster": "A", "Host": "h2", "OS according to the configuration file": "Windows 2019"},
    ]
    rows_b = rows_a + [
        {"VM": "api-01", "Powerstate": "poweredOff", "CPUs": 8, "Memory": 16384, "Provisioned MB": 120000, "In Use MB": 30000, "Cluster": "B", "Host": "h3", "OS according to the configuration file": "RHEL 8"}
    ]
    _write_manifest(tmp_path, "t1", rows_a, "2026-02-10T00:00:00+00:00")
    mp2, manifests_dir, chunks_dir = _write_manifest(tmp_path, "t2", rows_b, "2026-02-11T00:00:00+00:00")

    out = compute_advanced_analytics(str(mp2), str(manifests_dir), str(chunks_dir))
    assert "forecasting" in out
    assert "anomalies" in out
    assert "right_sizing_recommendations" in out
    assert "eos_prioritization" in out
    assert "consolidation_optimization" in out
    assert "dependency_graph" in out
    assert "drift_governance" in out
    assert "what_if_simulation" in out
    assert "operational_scorecard" in out
    assert "action_backlog" in out
