from pathlib import Path
from typing import Dict, Set, Optional
from src.schema.rvtools_schema import find_column
import json
import pandas as pd


def _find_column(df: pd.DataFrame, keywords):
    cols = [c for c in df.columns]
    low = {c.lower(): c for c in cols}
    for kw in keywords:
        for c_l, c_orig in low.items():
            if kw in c_l:
                return c_orig
    return None


def compute_full_kpis(manifests_dir: str, chunks_dir: str) -> Dict:
    mdir = Path(manifests_dir)
    cdir = Path(chunks_dir)
    manifests = list(mdir.glob('manifest_*.json'))
    result = {
        'total_vms': 0,
        'total_hosts': 0,
        'total_compute': 0,
        'total_memory_tb': '--',
        'eos_risk': 0,
        'vmtools_outdated': 0,
    }

    host_set: Set[str] = set()
    total_memory_mb = 0

    # Look for vInfo manifest (VMs) and host manifests
    for mf in manifests:
        try:
            m = json.loads(mf.read_text(encoding='utf-8'))
        except Exception:
            continue
        sheet = m.get('sheet', '')
        # process chunks
        for ch in m.get('chunks', []):
            local = ch.get('local_path')
            if not local:
                continue
            p = Path(local)
            if not p.exists():
                # may be stored in chunks_dir
                p = Path(chunks_dir) / ch.get('name')
                if not p.exists():
                    continue
            try:
                df = pd.read_parquet(p)
            except Exception:
                continue

            # VMs
            if sheet.lower().startswith('vinfo') or 'vm' in sheet.lower():
                result['total_vms'] += len(df)
                # detect host column via schema mapping
                host_col = find_column(df.columns, 'vInfo', 'host')
                if host_col and host_col in df.columns:
                    host_vals = df[host_col].dropna().unique().tolist()
                    host_set.update([str(h) for h in host_vals])
                # cpu
                cpu_col = find_column(df.columns, 'vInfo', 'numcpu')
                if cpu_col and cpu_col in df.columns:
                    try:
                        result['total_compute'] += int(df[cpu_col].fillna(0).astype(int).sum())
                    except Exception:
                        pass
                # memory
                mem_col = find_column(df.columns, 'vInfo', 'memorymb')
                if mem_col and mem_col in df.columns:
                    try:
                        total_mb = df[mem_col].fillna(0).astype(float).sum()
                        total_memory_mb += total_mb
                    except Exception:
                        pass
                # vmtools
                tools_col = find_column(df.columns, 'vInfo', 'vmtools')
                if tools_col and tools_col in df.columns:
                    s = df[tools_col].astype(str).str.lower()
                    outdated = s[s.str.contains('out') | s.str.contains('not') | s.str.contains('none')].count()
                    result['vmtools_outdated'] += int(outdated)

            # Hosts
            if sheet.lower().startswith('vhost') or 'host' in sheet.lower() or sheet.lower().startswith('host'):
                # total hosts if host rows present
                result['total_hosts'] += len(df)
                # try detect version
                ver_col = find_column(df.columns, 'vHost', 'version') or _find_column(df, ['version', 'esxi', 'product'])
                if ver_col:
                    try:
                        vers = df[ver_col].astype(str).dropna()
                        for v in vers:
                            v = v.strip()
                            if not v:
                                continue
                            # basic heuristic: major version number
                            if v[0] in ['6', '7']:
                                result['eos_risk'] += 1
                    except Exception:
                        pass
                # cpu
                cpu_col = find_column(df.columns, 'vHost', 'cpu') or _find_column(df, ['cpu', 'numcpu'])
                if cpu_col:
                    try:
                        result['total_compute'] += int(df[cpu_col].fillna(0).astype(int).sum())
                    except Exception:
                        pass
                # memory
                mem_col = find_column(df.columns, 'vHost', 'memorymb') or _find_column(df, ['memorymb', 'memory', 'mem'])
                if mem_col:
                    try:
                        total_mb = df[mem_col].fillna(0).astype(float).sum()
                        total_memory_mb += total_mb
                    except Exception:
                        pass

    # merge host set counts
    if host_set:
        result['total_hosts'] = max(result['total_hosts'], len(host_set))

    if total_memory_mb and total_memory_mb > 0:
        tb = total_memory_mb / 1024.0 / 1024.0
        result['total_memory_tb'] = f"{tb:.1f} TB"

    return result
