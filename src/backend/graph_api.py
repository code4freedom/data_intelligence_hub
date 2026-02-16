from fastapi import APIRouter
from fastapi.responses import JSONResponse
from pathlib import Path
import json
import pandas as pd
from src.backend.projects import ensure_project_dirs, normalize_project_name

router = APIRouter()


def build_graph_from_manifest(manifest_path: str):
    p = Path(manifest_path)
    if not p.exists():
        return {"nodes": [], "edges": []}
    m = json.loads(p.read_text(encoding='utf-8'))
    nodes = {}
    edges = []

    for ch in m.get('chunks', []):
        local = ch.get('local_path')
        if not local:
            continue
        pch = Path(local)
        if not pch.exists():
            continue
        try:
            df = pd.read_parquet(pch)
        except Exception:
            continue
        # heuristics to find VM/Host/Datastore/Network columns
        cols = [c.lower() for c in df.columns]
        col_map = {c.lower(): c for c in df.columns}
        # vm name
        vm_col = None
        for k in ['name', 'vmname', 'displayname']:
            if k in col_map:
                vm_col = col_map[k]
                break
        host_col = None
        for k in ['host', 'hostname', 'esxi']:
            if k in col_map:
                host_col = col_map[k]
                break
        ds_col = None
        for k in ['datastore', 'datastorename', 'ds']:
            if k in col_map:
                ds_col = col_map[k]
                break
        net_col = None
        for k in ['network', 'portgroup', 'nicnetwork']:
            if k in col_map:
                net_col = col_map[k]
                break

        for _, r in df.iterrows():
            vm = str(r[vm_col]) if vm_col and not pd.isna(r[vm_col]) else None
            host = str(r[host_col]) if host_col and not pd.isna(r[host_col]) else None
            ds = str(r[ds_col]) if ds_col and not pd.isna(r[ds_col]) else None
            net = str(r[net_col]) if net_col and not pd.isna(r[net_col]) else None
            if vm:
                if vm not in nodes:
                    nodes[vm] = {"id": vm, "type": "vm"}
            if host:
                if host not in nodes:
                    nodes[host] = {"id": host, "type": "host"}
            if ds:
                if ds not in nodes:
                    nodes[ds] = {"id": ds, "type": "datastore"}
            if net:
                if net not in nodes:
                    nodes[net] = {"id": net, "type": "network"}
            if vm and host:
                edges.append({"source": host, "target": vm, "type": "runs"})
            if vm and ds:
                edges.append({"source": vm, "target": ds, "type": "uses_storage"})
            if vm and net:
                edges.append({"source": vm, "target": net, "type": "connected_to"})

    return {"nodes": list(nodes.values()), "edges": edges}


@router.get('/graph')
def graph(manifest: str = 'manifest_vInfo_localtest.json', project: str = 'default'):
    p = normalize_project_name(project)
    dirs = ensure_project_dirs(p)
    mpath = dirs["manifests"] / manifest
    if not mpath.exists() and p == "default":
        legacy = Path('/data/manifests') / manifest
        if legacy.exists():
            mpath = legacy
    if not mpath.exists():
        return JSONResponse(status_code=404, content={"error": "manifest not found"})
    g = build_graph_from_manifest(str(mpath))
    return g
