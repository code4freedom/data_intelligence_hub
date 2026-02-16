from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Dict, List, Set

import pandas as pd


SENSITIVE_TOKENS = [
    "name",
    "vm",
    "host",
    "cluster",
    "datastore",
    "folder",
    "resourcepool",
    "resource pool",
    "ip",
    "dns",
    "fqdn",
    "path",
    "network",
    "portgroup",
    "switch",
    "vlan",
    "mac",
    "uuid",
    "serial",
    "annotation",
    "notes",
]

EXCLUDE_TOKENS = [
    "os according",
    "guest os",
    "version",
    "powerstate",
    "power state",
    "cpu",
    "memory",
    "provisioned",
    "in use",
]


def _norm_col(col: str) -> str:
    return " ".join(str(col).lower().replace("_", " ").split())


def _is_sensitive_column(col: str) -> bool:
    c = _norm_col(col)
    if any(t in c for t in EXCLUDE_TOKENS):
        return False
    return any(t in c for t in SENSITIVE_TOKENS)


def _prefix_for_col(col: str) -> str:
    c = _norm_col(col)
    if "vm" in c:
        return "VM"
    if "host" in c:
        return "HOST"
    if "cluster" in c:
        return "CLUSTER"
    if "datastore" in c:
        return "DS"
    if "folder" in c:
        return "FOLDER"
    if "network" in c or "portgroup" in c:
        return "NET"
    if "ip" in c:
        return "IP"
    return "MASK"


def _mask_str(value: str, seed: str, col: str) -> str:
    raw = value.strip()
    if not raw:
        return value
    digest = hashlib.sha1(f"{seed}|{col}|{raw}".encode("utf-8")).hexdigest()[:10].upper()
    return f"{_prefix_for_col(col)}-{digest}"


def anonymize_manifest_chunks(manifest: Dict, seed: str) -> Dict:
    """Mask sensitive string-like fields in chunk parquet files referenced by the manifest."""
    chunks = manifest.get("chunks", []) or []
    masked_columns: Set[str] = set()
    chunk_files: List[str] = []
    for ch in chunks:
        p = Path(str(ch.get("local_path", "")))
        if not p.exists():
            continue
        try:
            df = pd.read_parquet(p)
        except Exception:
            continue
        changed = False
        for col in df.columns:
            if not _is_sensitive_column(str(col)):
                continue
            # only mask textual values; keep numbers/dates untouched
            ser = df[col]
            if not (pd.api.types.is_object_dtype(ser) or pd.api.types.is_string_dtype(ser)):
                continue
            df[col] = ser.map(lambda v: _mask_str(v, seed, str(col)) if isinstance(v, str) else v)
            masked_columns.add(str(col))
            changed = True
        if changed:
            df.to_parquet(p, engine="pyarrow", index=False)
            chunk_files.append(str(p))

    out = dict(manifest)
    out["anonymized"] = True
    out["anonymized_at_utc"] = pd.Timestamp.utcnow().isoformat()
    out["anonymized_columns"] = sorted(masked_columns)
    out["anonymized_chunks"] = chunk_files
    return out
