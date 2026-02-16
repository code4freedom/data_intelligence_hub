from src.backend.kpis import compute_full_kpis
from tools.generate_synthetic import generate_vm_parquet
from pathlib import Path


def test_compute_kpis_tmp(tmp_path):
    out = tmp_path / 'chunks'
    generate_vm_parquet(str(out), num_vms=1000, chunk_size=200, ingest_id='unittest')
    manifests_dir = str(out)
    chunks_dir = str(out)
    k = compute_full_kpis(manifests_dir, chunks_dir)
    assert k['total_vms'] == 1000
    assert 'total_memory_tb' in k
