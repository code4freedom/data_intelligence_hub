import json
from pathlib import Path

import pandas as pd

from src.backend.intelligence import compute_manifest_intelligence


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


def test_enterprise_intelligence_with_trends(tmp_path: Path):
    rows_a = [
        {
            "VM": "sql-prod-01",
            "Powerstate": "poweredOn",
            "CPUs": 8,
            "Memory": 16384,
            "Provisioned MB": 150000,
            "In Use MB": 90000,
            "Cluster": "A",
            "Host": "esx-01",
            "OS according to the configuration file": "Windows 2012",
        },
        {
            "VM": "web-01",
            "Powerstate": "poweredOn",
            "CPUs": 4,
            "Memory": 8192,
            "Provisioned MB": 60000,
            "In Use MB": 45000,
            "Cluster": "A",
            "Host": "esx-02",
            "OS according to the configuration file": "Windows 2019",
        },
    ]
    rows_b = rows_a + [
        {
            "VM": None,
            "Powerstate": "poweredOn",
            "CPUs": 2,
            "Memory": 4096,
            "Provisioned MB": 20000,
            "In Use MB": 12000,
            "Cluster": "B",
            "Host": "esx-03",
            "OS according to the configuration file": "RHEL 8",
        },
        {
            "VM": "api-01",
            "Powerstate": "poweredOff",
            "CPUs": 8,
            "Memory": 16384,
            "Provisioned MB": 120000,
            "In Use MB": 30000,
            "Cluster": "B",
            "Host": "esx-03",
            "OS according to the configuration file": "RHEL 8",
        }
    ]

    _, manifests_dir, chunks_dir = _write_manifest(tmp_path, "day1", rows_a, "2026-02-10T00:00:00+00:00")
    mp2, _, _ = _write_manifest(tmp_path, "day2", rows_b, "2026-02-11T00:00:00+00:00")

    out = compute_manifest_intelligence(str(mp2), str(manifests_dir), str(chunks_dir))
    assert out["summary"]["total_vms"] == 4
    assert out["executive_score"] >= 0
    assert "consolidation" in out
    assert "lifecycle" in out
    assert "performance" in out
    assert "application" in out
    assert "mapping_coverage_pct" in out["application"]
    assert "unclassified_count" in out["application"]
    assert len(out["trends"]) >= 2
    assert "vms" in out["trend_growth"]
